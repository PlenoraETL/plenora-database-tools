# Driver SQL Server nell'API Rust

Le API di basso livello di `SqlServerSession` e `decode_row` espongono tipi
del driver SQL Server scelto dal [manifest workspace](../Cargo.toml).
Un consumer che costruisce direttamente query o righe del driver deve usare
lo stesso pacchetto e il medesimo pin: un tipo proveniente da `tiberius`
non e intercambiabile con il tipo omonimo di `tiberius-ng`.

Nel manifest del consumer si puo mantenere il nome locale `tiberius`
specificando `package = "tiberius-ng"` e copiando versione e feature dalla
dipendenza workspace. Ricompilare i consumer insieme al provider.
Questo vincolo non riguarda chi usa soltanto le astrazioni del core o il
binding Python.

Il driver proviene dal registro Cargo senza patch locali. Le prove del
provider verificano conteggi delle scritture con `NOCOUNT ON`, rifiuto dei
tipi fuori dal mapping pubblico, recupero delle sessioni e criteri TLS.
Il supporto di un tipo nel driver non abilita automaticamente una capability
di Plenora: fa fede il [documento generato](STATO.md).
