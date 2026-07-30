# Prestazioni SQL Server

La baseline post-RC1 misura il percorso reale della libreria contro SQL Server
2022 fissato per digest. La campagna comprende:

- read Arrow bounded;
- prepared write;
- TDS bulk;
- create atomico;
- replace con staged swap;
- differenziale bidirezionale fra sorgente e target per ogni scrittura.

Il comando riproducibile è:

```powershell
python scripts\check_sqlserver_performance.py `
  --baseline benchmarks/baseline/sqlserver2022-performance-reference.json `
  --output assurance-results/sqlserver-performance.json
```

Il manifest usa 2.000 righe, batch da 256, un warm-up e tre campioni misurati.
Il gate applica due livelli:

1. limiti assoluti conservativi di latenza, throughput e peak RSS;
2. regressione relativa rispetto alla baseline congelata.

La baseline iniziale del 2026-07-30 ha osservato:

| Percorso | p95 | throughput mediano |
|---|---:|---:|
| read | 72,593 ms | 27.660 righe/s |
| prepared | 510,933 ms | 6.767 righe/s |
| TDS bulk | 25,638 ms | 78.412 righe/s |
| create | 331,718 ms | 6.552 righe/s |
| replace | 321,991 ms | 6.522 righe/s |

Il differenziale è rimasto a zero e il peak RSS osservato è stato 6.574.080
byte. I numeri sono una baseline di regressione del profilo e dell'ambiente,
non una promessa universale di latenza. Il confronto relativo viene eseguito
solo quando piattaforma, architettura, CPU, immagini e campagna coincidono;
altrimenti il report dichiara `not_comparable` e applica soltanto il budget
assoluto.
