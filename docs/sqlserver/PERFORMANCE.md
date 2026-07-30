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

La baseline iniziale del 2026-07-30 ha osservato questi ordini di grandezza;
i numeri esatti autoritativi sono nel JSON congelato:

| Percorso | p95 | throughput mediano |
|---|---:|---:|
| read | circa 70 ms | circa 28.000 righe/s |
| prepared | circa 0,35 s | circa 6.000 righe/s |
| TDS bulk | circa 27 ms | circa 78.000 righe/s |
| create | circa 0,35 s | circa 5.900 righe/s |
| replace | circa 0,37 s | circa 5.600 righe/s |

Il differenziale è rimasto a zero e il peak RSS è rimasto sotto 7 MiB. I
numeri sono una baseline di regressione del profilo e dell'ambiente,
non una promessa universale di latenza. Il confronto relativo viene eseguito
solo quando piattaforma, architettura, CPU, immagini e campagna coincidono;
altrimenti il report dichiara `not_comparable` e applica soltanto il budget
assoluto.
