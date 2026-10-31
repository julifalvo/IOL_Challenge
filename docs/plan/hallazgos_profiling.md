# Hallazgos del profiling local

Salida de `python scripts/profile_local.py` sobre el CSV original (pandas, local). **Preliminar:** el notebook 01 debe recalcularlo en Spark y dejar outputs; citar en el README solo los números del notebook.

```text

## 1. Generales

filas=100,000  columnas=11
nulos por columna: {'fecha': 0, 'tipoTran': 0, 'id_cliente': 0, 'descripcion_titulo': 0, 'moneda': 0, 'simbolo_titulo': 0, 'cantidad': 0, 'precio': 0, 'id_transaccion': 0, 'origen': 0, 'date_local': 0}
timestamps no parseables: 0
id_transaccion duplicados: 0
timestamps repetidos: 1994
clientes=57,655  simbolos=1,445  descripciones=1,086
rango local: 2026-01-02 07:33:18-03:00 -> 2026-03-13 16:59:48-03:00
tipoTran: {'Compra': 70051, 'Venta': 29949}
moneda: {'ARS': 85420, 'USD': 14580}

## 2. Calendario (ADR-003)

dia   UTC     local
lun   9,155  11,506
mar  20,142  19,749
mie  19,099  19,178
jue  19,487  19,438
vie  19,619  19,874
sab  12,498  10,255
dom       0       0
días hábiles (lun-vie) sin operaciones: ['2026-02-16', '2026-02-17']
operaciones en feriados de config: 0
lunes: hora UTC min=10 max=23
sábado: hora UTC min=0 max=19
% dentro de rueda local 11-17 hs: 29.6%

## 3. Canales

{'App Mobile': 66438, 'Sitio Web Desktop': 28952, 'Sitio Web Responsive': 2651, 'API': 1688, 'IOLnet': 271}
clientes multicanal: 1,741

## 4. Clientes (ADR-007)

percentiles ops/cliente: {0.5: 1.0, 0.75: 2.0, 0.9: 3.0, 0.95: 4.0, 0.99: 10.0}  max=676
clientes con 1 operación: 40,841 (70.8%)

## 5. Instrumentos y variantes (ADR-004)

moneda                ARS    USD     All
asset_type_rule                         
BONO_SOBERANO       12446   5764   18210
BONO_SUBSOBERANO      229     34     263
CEDEAR              47610   6442   54052
LETRA_LECAP_BONCAP   3912     50    3962
NEEDS_REVIEW        18012   1229   19241
ON                   1292   1061    2353
OPCION               1824      0    1824
WARRANT                95      0      95
All                 85420  14580  100000
símbolos USD=471  con base ARS al quitar sufijo=279
símbolos ARS terminados en D/C (29): ['AMD', 'BA.C', 'BA37D', 'BB37D', 'BBD', 'BC37D', 'C', 'CARC', 'DD', 'ERIC', 'GILD', 'GLD', 'HD', 'HMC', 'HOOD', 'HSBC', 'INTC', 'JD', 'KGC', 'LAC', 'LND', 'MCD', 'PDD', 'SA24D', 'SID', 'VOD', 'WFC', 'XLC', 'YPFD']
símbolos con >1 descripción: ['AZN', 'IRS2W', 'X15Y6']
top 20 símbolos = 38.9% de operaciones: {'AL30': 8859, 'AL30D': 4589, 'MSFT': 2336, 'SPY': 2311, 'AMZN': 2054, 'MELI': 1881, 'PAMP': 1668, 'NVDA': 1493, 'YPFD': 1485, 'GOOGL': 1453, 'COME': 1350, 'GLD': 1263, 'AAPL': 1110, 'TXAR': 1101, 'ALUA': 1059, 'BYMA': 1058, 'GGAL': 993, 'VIST': 967, 'B': 926, 'HMY': 907}

## 6. Escala de precios (ADR-005)

{'AL30': 876.3192, 'AL30D': 0.6073, 'AL30C': 0.5904}
{'GD30': 911.583, 'GD30D': 0.6182}
{'AAPL': 19625.8112, 'AAPLD': 13.4356, 'AAPLC': 13.1884}
MEP implícito (mediana AL30/AL30D): 1,443.0

## 7. Extremos (DQ020/DQ021)

top 5 por cantidad:
 simbolo_titulo                            descripcion_titulo origen moneda   cantidad  precio       amount
         T30A7                BONO TESORO NAC CAP V.30/04/27 IOLnet    ARS 1503210011  0.0101 1.518242e+07
         T31Y7 BONO TESORO NACIONAL CAPITALIZABLE 31/05/27 $ IOLnet    ARS 1427140785  0.0100 1.427141e+07
         TTS26                 BONO NACION TASA DUAL15/09/26 IOLnet    ARS 1067195974  1.3802 1.472944e+09
         T30J6                   BONCAP T30J6 VTO 30/06/2026 IOLnet    ARS  976270797  1.2550 1.225220e+09
         T13F6                    BONCAP 13F6 VTO 13/02/2026 IOLnet    ARS  661750591  1.4238 9.422005e+08

filas con precio a x50 o x1/50 de la mediana del símbolo: 13
origen                asset_type_rule   
API                   OPCION                1
IOLnet                BONO_SOBERANO         3
                      CEDEAR                1
                      LETRA_LECAP_BONCAP    1
                      NEEDS_REVIEW          2
                      ON                    1
Sitio Web Desktop     OPCION                3
Sitio Web Responsive  OPCION                1
IOLnet con precio ≈1,00 o 0,01: 17 de 271

cantidad/precio <= 0: 0 / 0

## 8. Actividad por mes

mes 1: 47,562 ops, 21 días hábiles con datos, 2,265 ops/día hábil
mes 2: 35,171 ops, 18 días hábiles con datos, 1,954 ops/día hábil
mes 3: 17,267 ops, 10 días hábiles con datos, 1,727 ops/día hábil
```

## Notas de interpretación (fuera de la salida del script)

- **Filas con escala anómala (§7).** [MEDIDO] De las 13 filas a ×50 o ×1/50 de la mediana del símbolo, 8 son de IOLnet. Las 8 tienen precio que redondea a 0,01 o 1,00: `SATL` 1,0046, `TGNO4` 0,9999, `METR` 0,9997, `T31Y7` 0,0100, `AL30` 0,0100, `DNC7D` 0,0100, `T13F6` 0,0100 y `T30A7` 0,0101.
  - La línea "IOLnet con precio ≈1,00 o 0,01: 17 de 271" es otra métrica: recorre todas las filas de IOLnet y exige 0,01 exacto, por eso no cuenta a `T30A7`.
  - [HIPÓTESIS] Que sea un valor por defecto en la carga de IOLnet. No está confirmado; ver ADR-005.
