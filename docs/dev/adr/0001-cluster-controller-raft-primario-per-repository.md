# ADR-0001: Cluster Gogs senza dipendenze esterne, con controller Raft e primario per repository

- **Stato:** proposta
- **Data:** 2026-10-19
- **Ultimo aggiornamento:** 2026-10-20, scelta dello storage locale (Pebble per i log, SQLite per lo stato)
- **Decisori:** BartokIT

## Contesto

Gogs oggi è un processo singolo. I repository sono bare repository sul disco locale (`[repository] ROOT`, path calcolati in [`internal/repox/repox.go`](../../../internal/repox/repox.go)), i metadati stanno in un database (SQLite, PostgreSQL o MySQL), e sessioni e cache vivono in memoria (`[session] PROVIDER = memory`, `[cache] ADAPTER = memory` in [`conf/app.ini`](../../../conf/app.ini)). L'alta affidabilità oggi si ottiene solo con infrastruttura esterna: un database gestito e uno storage condiviso.

L'obiettivo è un server Git distribuito con queste proprietà:

1. Almeno 3 nodi, anche in siti geograficamente distanti.
2. Un unico binario: nessun database esterno, nessuno storage a oggetti (per esempio S3), nessun filesystem distribuito, nessun coordinatore esterno.
3. Un push confermato al client non va mai perso, e `main` non può divergere tra nodi diversi.
4. Il cluster sopravvive alla perdita di un nodo o di un intero sito.
5. Le letture (clone, fetch, pagine web) sono servite dal nodo più vicino.
6. Il sistema scala sul numero di repository, che è la dimensione che cresce in una forge.

Lo stato di una forge si divide in due parti con problemi opposti:

| Parte | Esempi | Dimensione | Problema |
|---|---|---|---|
| Stato mutabile | ref (dove punta `main`), utenti, permessi, issue, PR | piccola | va ordinato: due scritture concorrenti richiedono una decisione condivisa da tutti i nodi, cioè consenso |
| Dati immutabili | oggetti Git, oggetti LFS, allegati | grande | nessun conflitto possibile: il nome è l'hash del contenuto, quindi si copiano da qualunque nodo e si verificano localmente |

Git offre già un protocollo efficiente e verificabile per replicare la seconda parte: `git fetch`. La prima parte richiede un protocollo di consenso incorporato nel binario.

## Decisione

Adottiamo un'architettura a due livelli, modellata sul protocollo di replica di Kafka con KRaft:

- un **controller**: un unico gruppo Raft, eseguito dagli stessi nodi Gogs, che conserva i metadati globali e decide, per ogni repository, quale nodo è il **primario**.
- per ogni repository, un **primario** e delle **repliche**, con un **log di replica** per repository, un insieme di **repliche in sync** (ISR), un **high watermark** (HW) e un'**epoca** che protegge dai primari obsoleti.

Gli oggetti Git e LFS non passano mai dal consenso: viaggiano via `git fetch` (o richieste HTTP per LFS) e vengono verificati per hash. Nel consenso e nei log passano solo i loro hash.

Su ogni nodo, tutti i log e i metadati di replica stanno in un'unica istanza [Pebble](https://github.com/cockroachdb/pebble), mentre lo stato relazionale materializzato dai log sta in SQLite. I dettagli sono nella sezione [Storage locale](#storage-locale).

### Controller

Il controller è un gruppo Raft di 3 o 5 nodi votanti, eseguito all'interno dei normali nodi Gogs, come il "combined mode" di KRaft. Il suo log replicato contiene:

- l'appartenenza al cluster e lo stato di vita dei nodi.
- la **tabella di placement**: per ogni repository, le repliche assegnate, il primario, l'ISR e l'epoca corrente.
- i comandi sul **database globale**: utenti, organizzazioni, team, permessi, chiavi SSH e token.

Ogni nodo invia un heartbeat al controller (per esempio ogni secondo). Se un nodo non si fa sentire entro il timeout di sessione (per esempio 9 secondi, il default di Kafka), il controller lo considera morto e riassegna i primari che ospitava. Il costo del rilevamento dei guasti è quindi proporzionale al numero di nodi, non al numero di repository.

Il log Raft del controller e il suo stato interno (termine, voto, configurazione) stanno in Pebble. Lo stato che il log descrive viene materializzato su ogni nodo votante nel database SQLite globale. Uno snapshot Raft è una copia di quel database più la coda del log successiva alla copia.

### Primario, repliche e ISR

Ogni repository ha un fattore di replica (default 3) e un ISR minimo (default 2). Il controller sceglie le repliche in domini di guasto diversi (siti o zone configurabili) e ne nomina una primario.

- Il **primario** riceve tutte le scritture del repository e le aggiunge al log di replica.
- Le **repliche** seguono il primario in modalità pull: chiedono le voci successive all'ultima che conoscono, prendono gli oggetti mancanti con `git fetch` dal primario e confermano.
- L'**ISR** è l'insieme delle repliche allineate. Una replica che resta indietro oltre una soglia di tempo esce dall'ISR, ma solo tramite il controller.
- L'**high watermark** è l'ultima voce del log presente su tutti i membri dell'ISR. Solo le voci fino all'HW sono confermate.
- Le **repliche osservatrici** sono copie asincrone fuori dall'ISR. Servono letture locali in siti lontani e disaster recovery, senza rallentare le scritture.

Una rete di fork (un repository e tutti i suoi fork) ha sempre lo stesso insieme di repliche, perché le pull request tra fork richiedono entrambi i repository sullo stesso disco.

### Log di replica per repository

Ogni repository ha un log ordinato di voci. Una voce ha questa forma:

```json
{
  "repo_id": 42,
  "seq": 1287,
  "epoch": 7,
  "kind": "ref_update",
  "updates": [
    { "ref": "refs/heads/main", "old": "4a1c…", "new": "9f2e…" }
  ]
}
```

Il campo `kind` distingue gli aggiornamenti di ref dai comandi sui dati del repository (per esempio `issue_create`, `comment_create`). I comandi contengono già tutti i valori calcolati dal primario, inclusi timestamp e identificativi, così che l'applicazione su ogni replica sia deterministica.

**Il log è la fonte di verità. I ref su disco e le righe nel database locale sono una sua materializzazione fino all'HW.** Un nodo che riparte ricostruisce i ref rileggendo il log dall'ultimo punto applicato.

### Percorso di scrittura

Un push segue questi passi:

1. Il client si collega al nodo più vicino, via HTTP ([`serviceRPC`](../../../internal/route/repo/http.go)) o SSH ([`runServ`](../../../cmd/gogs/serv.go)).
2. Se quel nodo non è il primario del repository, inoltra la richiesta al primario indicato dalla tabella di placement.
3. Sul primario, `git receive-pack` riceve il pack e lo tiene in quarantena (`GIT_QUARANTINE_PATH`, Git 2.13 e successivi) mentre esegue gli hook.
4. Nell'hook pre-receive ([`runHookPreReceive`](../../../cmd/gogs/hook.go)), dopo i controlli esistenti sui branch protetti, il primario:
   1. verifica il compare-and-swap di ogni ref rispetto allo stato in testa al log, non rispetto al disco.
   2. aggiunge la voce al log con l'epoca corrente.
   3. attende che tutti i membri dell'ISR abbiano confermato sia la voce sia gli oggetti a cui punta.
   4. esce con successo quando l'HW raggiunge la voce, con errore se scade il timeout o se l'ISR scende sotto il minimo.
5. `receive-pack` aggiorna i ref locali e risponde al client.
6. Gli effetti collaterali (webhook, feed delle attività, test delle pull request, oggi avviati da [`runHookPostReceive`](../../../cmd/gogs/hook.go) e [`TriggerTask`](../../../internal/route/repo/tasks.go)) li esegue solo il primario, dopo l'HW, usando `(repo_id, seq)` come chiave di idempotenza.

Due push concorrenti sullo stesso ref vengono serializzati dal log: il secondo fallisce il compare-and-swap al passo 4.1 e il client riceve `rejected (non-fast-forward)`, come su un server singolo.

Le scritture web che riguardano un repository (issue, commenti, impostazioni) seguono lo stesso percorso, come comandi nel log del repository. Le scritture globali (utenti, permessi) passano dal leader del controller.

### Percorso di lettura

Ogni replica e ogni osservatrice serve clone, fetch e pagine web con lo stato fino al proprio HW. Le letture possono quindi essere indietro di qualche istante rispetto al primario. Quando serve leggere ciò che si è appena scritto (per esempio la pagina mostrata subito dopo la creazione di una issue), il nodo inoltra la lettura al primario oppure attende di aver applicato la voce indicata dal primario.

### Failover e fencing

Quando il controller dichiara morto un primario:

1. sceglie il nuovo primario **solo tra i membri dell'ISR**.
2. incrementa l'epoca del repository e registra il nuovo primario e il nuovo ISR nel proprio log.
3. comunica la nuova epoca alle repliche.

Un primario isolato ma ancora acceso (il "primario zombie") può ancora accettare una connessione, ma non può confermare nulla: le repliche conoscono l'epoca nuova e rifiutano ogni voce con epoca inferiore, quindi il suo HW non avanza e il client non riceve mai una conferma. Per limitare le attese dei client, un primario che non riesce a contattare il controller per un intervallo configurabile smette da solo di accettare scritture.

Quando il vecchio primario torna, chiede al nuovo primario qual è l'ultima voce della propria ultima epoca, tronca il log a quel punto e riporta i ref allo stato corrispondente. Questo è l'approccio di Kafka (KIP-101 e KIP-279), preferito al semplice troncamento all'HW, che può perdere o far divergere voci. Gli oggetti in più rimasti sul disco sono innocui e vengono rimossi dal normale `git gc`.

Un cambio pianificato del primario (per esempio per avvicinarlo agli utenti che scrivono di più) usa lo stesso meccanismo, scegliendo un membro dell'ISR già allineato.

### Regole di sicurezza

Queste invarianti valgono in ogni momento. Ogni implementazione e ogni test di guasto le verifica.

1. **Elezione pulita.** Il nuovo primario si sceglie solo tra i membri dell'ISR. Non esiste un'elezione di una replica non allineata, nemmeno manuale, senza un'operazione amministrativa esplicita che dichiari la possibile perdita di dati.
2. **ISR gestito dal controller.** Il primario può chiedere di togliere o aggiungere una replica, ma la modifica vale solo quando il controller la registra. Altrimenti un primario potrebbe confermare scritture su un ISR ridotto, morire, e lasciare promuovere una replica che non le ha.
3. **ISR minimo.** Con fattore di replica 3, l'ISR minimo è 2. Se l'ISR scende sotto il minimo, le scritture vengono rifiutate invece di essere confermate su un solo disco.
4. **Epoca su ogni messaggio.** Ogni voce, richiesta di replica e conferma porta l'epoca. Chi riceve un'epoca inferiore a quella che conosce rifiuta il messaggio.
5. **Oggetti prima dei ref.** Una voce è confermata solo quando ogni membro dell'ISR ha sia la voce sia tutti gli oggetti raggiungibili dai nuovi valori dei ref. Nessun ref, su nessun nodo, punta mai a un oggetto assente.
6. **Nessuna esposizione oltre l'HW.** Un nodo non mostra ai client ref o dati del repository oltre il proprio HW.
7. **Effetti collaterali una sola volta.** Webhook, cron, sincronizzazione dei mirror e altri effetti verso l'esterno li esegue un solo nodo: il primario per quelli di un repository, il leader del controller per quelli globali. Ogni effetto porta una chiave di idempotenza.
8. **Determinismo.** I comandi nei log contengono valori già calcolati. Nessuna replica genera timestamp, numeri casuali o identificativi durante l'applicazione.
9. **Applicazione atomica.** Le modifiche prodotte da una voce e l'indice "applicato fino a `seq`" vengono scritti in SQLite nella stessa transazione. Dopo un crash un nodo riprende dall'indice salvato, senza applicare due volte la stessa voce e senza saltarne nessuna.

### Dati del database

Il database attuale viene diviso in due parti, entrambe SQLite in puro Go (già supportato tramite `glebarez/sqlite` nel [`go.mod`](../../../go.mod)):

- **database globale**: utenti, organizzazioni, team, permessi, chiavi, token, tabella di placement. È materializzato dal log del controller.
- **database locale dei repository**: issue, pull request, commenti, release, etichette, milestone. Ogni nodo ne ha uno, con i dati dei repository che ospita, materializzato dai log dei singoli repository.

Il traffico effimero resta fuori dai log: le sessioni diventano cookie firmati, la cache resta locale con TTL brevi, e contatori come le visite o l'ultimo accesso vengono aggregati e scritti periodicamente.

Le viste che attraversano molti repository (dashboard, ricerca, "le mie issue", statistiche di amministrazione) non possono più usare `JOIN` su un unico database. Diventano query distribuite sui nodi oppure indici globali aggiornati in modo asincrono.

### Storage locale

Ogni nodo usa due motori di storage incorporati, entrambi in puro Go, ciascuno per la forma di dati per cui è progettato.

**Pebble per i log e i metadati di replica.** Un'unica istanza Pebble per nodo contiene:

- il log Raft del controller e il suo stato interno.
- i log di replica di tutti i repository ospitati dal nodo.
- per ogni repository, l'epoca corrente, l'HW e l'ultima voce ricevuta.

Le chiavi hanno un prefisso per tipo e per repository, con i numeri codificati in big-endian così che l'ordine dei byte coincida con l'ordine numerico:

```text
raft/log/<index>                 voci del log del controller
raft/state/<nome>                termine, voto, configurazione
repo/<repo_id>/log/<seq>         voci del log di replica
repo/<repo_id>/meta/<nome>       epoca, HW, ultima voce ricevuta
```

Questa disposizione rende semplici le operazioni frequenti:

- leggere le voci successive a una data `seq` è un iteratore su un intervallo di chiavi.
- compattare un log o troncarlo dopo un failover è una sola cancellazione di intervallo.
- spostare un repository su un altro nodo significa esportare il suo intervallo di chiavi in file SST e importarli a destinazione.
- molte scritture piccole e concorrenti, di repository diversi, vengono raggruppate in un unico fsync del WAL di Pebble.

**SQLite per lo stato materializzato.** Il database globale e il database locale dei repository restano SQLite, con lo schema relazionale e le query attuali di [`internal/database`](../../../internal/database). Gogs oggi ha 38 tabelle e centinaia di query con filtri, `JOIN`, ordinamenti e paginazione. Riscriverle su un modello chiave-valore non porterebbe benefici all'architettura.

**Durabilità.** Una voce è al sicuro quando è nel WAL di Pebble su tutti i membri dell'ISR, oppure sulla maggioranza per il log del controller. SQLite è una materializzazione ricostruibile dai log, quindi può usare `synchronous=NORMAL` e risparmiare un fsync per scrittura. Questo vale a una condizione: i log vanno conservati almeno fino all'ultimo snapshot di SQLite. Uno snapshot completo di un nodo è una copia dei database SQLite (con `VACUUM INTO` o l'API di backup) più la coda dei log successiva alla copia.

Alternative valutate per lo storage locale:

| Opzione | Esito | Motivo |
|---|---|---|
| Solo SQLite, log inclusi | scartata | milioni di log append-only diventano tabelle enormi, con troncamenti e compattazioni riga per riga |
| Solo Pebble, stato incluso | scartata | richiede di riscrivere tutto il livello dati di Gogs come modello chiave-valore, con indici secondari, conteggi e migrazioni gestiti a mano |
| bbolt per i log | scartata | un solo scrittore alla volta, scritture amplificate con log grandi, e il file non si restringe dopo le cancellazioni |
| Pebble per i log, SQLite per lo stato | **scelta** | ogni motore gestisce la forma di dati per cui è progettato, come in rqlite (log Raft in un KV, stato in SQLite) |

### Componenti di Gogs da adattare

| Componente | Oggi | Nel cluster |
|---|---|---|
| Push HTTP e SSH | eseguiti localmente | inoltrati al primario, conferma nell'hook pre-receive dopo l'HW |
| Hook (`gogs hook`) | scrivono direttamente nel database | passano dal primario tramite un'API interna |
| Sessioni | in memoria | cookie firmati |
| Cache | in memoria | locale, con TTL brevi |
| Cron ([`internal/cron`](../../../internal/cron/cron.go)) | eseguiti da ogni processo | eseguiti dal leader del controller o dal primario del repository |
| Mirror ([`SyncMirrors`](../../../internal/database/mirror.go)) | sincronizzati localmente | sincronizzati dal primario, il risultato entra nel log come aggiornamento di ref |
| Webhook ([`internal/database/webhook.go`](../../../internal/database/webhook.go)) | consegnati da ogni processo | consegnati dal primario con chiave di idempotenza |
| LFS ([`internal/lfsx/storage.go`](../../../internal/lfsx/storage.go)) | file locali | file immutabili, replicati sull'ISR prima di confermare l'upload |
| Allegati e avatar | file locali | come LFS |
| Chiavi host SSH ([`internal/ssh/ssh.go`](../../../internal/ssh/ssh.go)) | generate per nodo | uguali su tutti i nodi, conservate nel controller |
| `git gc` | locale | resta locale e indipendente su ogni nodo |
| Avvio dei servizi ([`initServices`](../../../cmd/gogs/internal/web/web.go)) | coda e worker locali | in più: membro del controller, follower dei log, worker di replica |

## Opzioni considerate

### Opzione A: storage condiviso e database esterno

L'alta affidabilità attuale di Gogs e Gitea: più processi dietro un bilanciatore, repository su NFS o Ceph, database PostgreSQL o MySQL esterno.

| Dimensione | Valutazione |
|---|---|
| Complessità per Gogs | bassa |
| Dipendenze esterne | molte: filesystem distribuito e database |
| Scalabilità | limitata dal filesystem condiviso |
| Coerenza delle scritture | delegata al filesystem e al database |

**Pro:** quasi nessuna modifica al codice. **Contro:** viola il requisito di un unico binario. Git su NFS è fragile (lock, latenza dei metadati), e la distribuzione geografica di un filesystem condiviso è difficile.

### Opzione B: storage a oggetti e PostgreSQL

Il modello di GitLab: oggetti su S3 o compatibile, metadati su PostgreSQL, eventualmente un livello di replica dei repository come Gitaly Cluster.

| Dimensione | Valutazione |
|---|---|
| Complessità per Gogs | alta |
| Dipendenze esterne | molte: storage a oggetti, PostgreSQL |
| Scalabilità | alta |
| Coerenza delle scritture | delegata a PostgreSQL |

**Pro:** architettura provata su larga scala. **Contro:** viola il requisito di un unico binario, ed è la dipendenza che il progetto vuole eliminare.

### Opzione C: distribuzione dei contenuti via BitTorrent o IPFS

Bundle Git o pack distribuiti come torrent tra i nodi o verso i client, con web seed HTTP.

| Dimensione | Valutazione |
|---|---|
| Complessità per Gogs | media |
| Dipendenze esterne | nessuna |
| Scalabilità | alta per letture di contenuti grandi e popolari |
| Coerenza delle scritture | **nessuna soluzione** |

**Pro:** riduce la banda per clone da zero di repository grandi e pubblici. **Contro:** risolve solo la parte immutabile. Non offre consenso sui ref né un percorso di scrittura. I pack generati dal protocollo smart sono diversi per ogni client e quindi non condivisibili. Tra pochi nodi noti e fidati, tracker, DHT e tit-for-tat non aggiungono valore rispetto a `git fetch`. Resta possibile in futuro come livello opzionale e indipendente di distribuzione verso i client, tramite bundle immutabili e `bundle-uri`.

### Opzione D: replica asincrona primario-secondario

Un primario accetta le scritture e le invia ai secondari con push mirror, senza attendere conferme.

| Dimensione | Valutazione |
|---|---|
| Complessità per Gogs | bassa |
| Dipendenze esterne | nessuna |
| Scalabilità | limitata dal primario unico |
| Coerenza delle scritture | debole |

**Pro:** semplice. **Contro:** al failover si perdono le scritture non ancora replicate, e senza consenso il rischio di split brain è concreto. Viola il requisito 3.

### Opzione E: un unico gruppo Raft con replica completa

Tutte le scritture, ref e database, passano da un unico log Raft. Ogni nodo ha una copia completa di tutto.

| Dimensione | Valutazione |
|---|---|
| Complessità per Gogs | media |
| Dipendenze esterne | nessuna |
| Scalabilità | limitata da ciò che sta su un nodo e dal leader unico |
| Coerenza delle scritture | forte, attesa sulla maggioranza più veloce |

**Pro:** il modello più semplice con coerenza forte. **Contro:** ogni nodo deve contenere tutti i repository, e tutte le scritture del cluster passano da un solo leader, anche quelle di team lontani da esso.

### Opzione F: Multi-Raft a shard

Un gruppo Raft globale e un numero fisso di shard, ognuno con il suo gruppo Raft, i suoi repository e il suo leader, come CockroachDB o TiKV.

| Dimensione | Valutazione |
|---|---|
| Complessità per Gogs | alta |
| Dipendenze esterne | nessuna |
| Scalabilità | alta |
| Coerenza delle scritture | forte, attesa sulla maggioranza più veloce |

**Pro:** scala spazio e scritture, sopporta guasti senza un coordinatore centrale. **Contro:** molti gruppi Raft da gestire (heartbeat, elezioni, snapshot per gruppo). La leadership è per shard, quindi tutti i repository di uno shard condividono lo stesso leader e lo spostamento verso i propri utenti è grossolano.

### Opzione G: controller Raft e primario per repository (scelta)

Descritta nella sezione [Decisione](#decisione).

| Dimensione | Valutazione |
|---|---|
| Complessità per Gogs | alta |
| Dipendenze esterne | nessuna |
| Scalabilità | alta: un log e una riga di metadati per repository |
| Coerenza delle scritture | forte, attesa su tutto l'ISR |

**Pro:** un solo gruppo di consenso, rilevamento dei guasti per nodo, failover e cambio del primario per singolo repository. La replica pull coincide con `git fetch`. Il modello è provato in produzione da Kafka su milioni di partizioni, e i suoi casi limite sono documentati. **Contro:** ogni scrittura attende la replica in sync più lenta, e il failover dipende dalla disponibilità del controller.

### Opzione H: namespace per utente e replica senza consenso

Il modello di Radicle: ogni partecipante ha i propri ref firmati, replicati per gossip, senza un `main` ufficiale.

**Pro:** nessun consenso, funziona anche durante partizioni prolungate. **Contro:** non esiste una versione autorevole del repository. È un prodotto diverso da una forge con permessi, branch protetti e pull request.

## Analisi dei compromessi

**Attesa sull'ISR contro attesa sulla maggioranza.** Raft conferma una scrittura quando risponde la maggioranza più veloce. L'ISR la conferma quando hanno risposto tutte le repliche in sync, quindi la latenza è quella della replica più lenta. Con repliche in siti lontani questo pesa su ogni push. Le contromisure sono tre, combinabili: il controller colloca l'ISR di ogni repository in siti vicini tra loro e al primario, le repliche lontane diventano osservatrici asincrone, e una replica lenta esce dall'ISR (tramite il controller) accettando temporaneamente una durabilità a 2 copie.

**Dipendenza dal controller.** Se il controller perde la maggioranza, i primari esistenti continuano a servire letture e scritture finché l'ISR non deve cambiare, ma nessun failover è possibile. Il controller va quindi eseguito su 3 o 5 nodi in siti diversi, e la sua indisponibilità va monitorata come un incidente.

**Durabilità e disponibilità.** Con fattore di replica 3 e ISR minimo 2, il sistema accetta scritture con un nodo perso e le rifiuta con due nodi persi. È lo stesso comportamento di un gruppo Raft da 3 nodi, scelto deliberatamente: rifiutare un push è preferibile a confermarlo su un solo disco.

**Spazio disco.** Ogni repository occupa spazio pari al fattore di replica, più le osservatrici. È il prezzo dell'assenza di uno storage condiviso.

**Familiarità.** Raft e il protocollo di replica di Kafka sono ben documentati, ma nessuna forge esistente adotta questa combinazione in un unico binario. GitHub (Spokes) usa un protocollo di commit proprio su 3 repliche, GitLab (Gitaly Cluster) si appoggia a PostgreSQL, Gerrit multi-site a Kafka e Zookeeper. Il progetto non può copiare una soluzione esistente e va validato con test di guasto.

## Conseguenze

Diventa più facile:

- installare un cluster: un solo binario e un file di configurazione per nodo.
- sopravvivere alla perdita di un nodo o di un sito senza perdere push confermati.
- servire letture dal nodo più vicino.
- spostare un singolo repository o il suo primario verso i suoi utenti.
- aggiungere capacità aggiungendo nodi.

Diventa più difficile:

- ogni scrittura nel database passa da un log, quindi il codice di [`internal/database`](../../../internal/database) va riorganizzato in comandi deterministici.
- le viste globali richiedono query distribuite o indici asincroni.
- le migrazioni dello schema vanno coordinate tra nodi con versioni diverse del binario durante un aggiornamento progressivo.
- il debug di problemi di replica richiede strumenti nuovi (stato dei log, ISR, epoche, ritardo delle repliche).
- ogni nodo gestisce due motori di storage. Il formato su disco di Pebble si aggiorna solo in avanti, quindi un aggiornamento del binario va pianificato come una migrazione, e le chiavi dei log si ispezionano solo con strumenti dedicati.

Da rivalutare in seguito:

- la libreria Raft, dopo il prototipo della fase 1.
- se la latenza geografica dell'ISR diventa un problema, una conferma a maggioranza per i repository con repliche lontane.
- la distribuzione dei clone verso i client tramite bundle (opzione C), come livello indipendente.

## Fasi

Ogni fase si chiude con un criterio verificabile.

1. **Preparazione su nodo singolo.** Introdurre la funzione di lookup del placement (inizialmente sempre il nodo locale), separare il database globale da quello dei repository, trasformare le scritture in comandi deterministici, passare le sessioni a cookie firmati.
   - *Criterio:* tutti i test esistenti passano, e un'installazione singola si comporta come oggi.
2. **Controller.** Gruppo Raft su 3 nodi con il log in Pebble e il database globale in SQLite, inoltro delle scritture globali al leader, cron globali solo sul leader.
   - *Criterio:* con 3 nodi locali, uccidere il leader durante scritture continue non perde nessuna scrittura confermata.
3. **Log per repository.** Primario, repliche pull, ISR, HW ed epoca, con i log nell'istanza Pebble del nodo. Il push viene confermato nell'hook pre-receive, e le repliche seguono via `git fetch`.
   - *Criterio:* dopo ogni push confermato, `git rev-parse --all` e `git fsck` danno lo stesso risultato su tutti i membri dell'ISR.
4. **Failover e operazioni.** Fencing, troncamento per epoca, cambio pianificato del primario, ingresso e uscita dei nodi, strumenti di stato del cluster.
   - *Criterio:* isolare il primario durante push continui non produce mai due versioni diverse di un ref confermato.
5. **Dati dei repository.** Issue, PR, commenti e release come comandi nel log del repository, replica di LFS e allegati, webhook con chiave di idempotenza.
   - *Criterio:* le stesse verifiche della fase 3 valgono per le righe del database locale dei repository.
6. **Test di guasto e geografia.** Test in stile Jepsen (nodi uccisi a metà push, partizioni simulate, controller irraggiungibile), repliche osservatrici, regole di placement per sito.
   - *Criterio:* nessuna violazione delle [regole di sicurezza](#regole-di-sicurezza) su un numero concordato di esecuzioni casuali.

## Domande aperte

- Libreria Raft per il controller: `hashicorp/raft` (API di alto livello, usata da Consul e Nomad, richiede un adattatore Pebble per `LogStore` e `StableStore`) o `etcd-io/raft` (più flessibile, con lo storage da implementare comunque, più codice da scrivere).
- Politica di compattazione dei log per repository: ogni quante voci, o dopo quanto tempo, conservare solo la coda successiva all'ultimo snapshot di SQLite.
- Valori di default per timeout di sessione, ritardo massimo nell'ISR e timeout di attesa dell'HW nel push.
- Migrazione di un'installazione esistente a nodo singolo verso il cluster.
- Politica di placement: come esprimere siti e zone nella configurazione, e come ribilanciare quando si aggiunge un nodo.

## Riferimenti

- Diego Ongaro e John Ousterhout, *In search of an understandable consensus algorithm* (2014), <https://raft.github.io/>.
- Kafka KIP-101, *Alter replication protocol to use leader epoch rather than high watermark for truncation*.
- Kafka KIP-279, *Fix log divergence between leader and follower after fast leader fail over*.
- Kafka KIP-500, *Replace ZooKeeper with a self-managed metadata quorum*.
- Kafka KIP-595, *A Raft protocol for the metadata quorum*.
- Kafka KIP-631, *The quorum-based Kafka controller*.
- Sanjay Ghemawat, Howard Gobioff e Shun-Tak Leung, *The Google file system* (2003), per il modello master e primario con lease.
- Documentazione Git sugli hook, in particolare pre-receive, reference-transaction e l'ambiente di quarantena: <https://git-scm.com/docs/githooks>.
- `hashicorp/raft`: <https://github.com/hashicorp/raft>.
- `etcd-io/raft`: <https://github.com/etcd-io/raft>.
- rqlite, come riferimento per la replica di SQLite via Raft: <https://github.com/rqlite/rqlite>.
- Pebble, il motore di storage chiave-valore di CockroachDB: <https://github.com/cockroachdb/pebble>.
