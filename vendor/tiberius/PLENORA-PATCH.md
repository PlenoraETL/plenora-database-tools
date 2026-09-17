# Patch TLS locale

Base: crate tiberius 0.12.3, SHA-256 `a1446cb4198848d1562301a3340424b4f425ef79f35ef9ee034769a9dd92c10d`.
Il trasporto rustls deriva da prisma/tiberius, commit
`feb8df25afddb64282b7b886cab49254e3dee628`.

Il manifest usa tokio-rustls 0.26 e rustls-native-certs 0.8, senza
rustls-pemfile. Il provider crittografico e ring, scelto esplicitamente.
Un archivio di CA vuoto restituisce un errore invece di provocare un panic.
La policy di verifica resta quella selezionata dal chiamante: Verify come
default, TrustServerCertificate solo su richiesta esplicita.

La patch viene verificata dai gate SQL Server del repository principale.
Rimuovere il backport quando una release upstream con questi cambiamenti
supera gli stessi gate. Cargo.toml.orig conserva il manifest upstream.
