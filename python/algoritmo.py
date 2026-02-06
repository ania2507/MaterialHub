import os
import json
import re
import pandas as pd
import logging
from dotenv import load_dotenv
from hdbcli import dbapi
import time
import math
import difflib
from collections import defaultdict
from datetime import date

# ===========================
# CONFIGURAZIONE LOGGING
# ===========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
start_time = time.time()
load_dotenv(dotenv_path=".env")
# ===========================
# FUNZIONI UTILI
# ===========================
def get_hana_credentials():
    vcap = os.getenv("VCAP_SERVICES")
    if vcap:
        services = json.loads(vcap)
        creds = services["hana"][0]["credentials"]
    else:
        creds = {
            "host": os.getenv("HANA_HOST"),
            "port": os.getenv("HANA_PORT"),
            "user": os.getenv("HANA_USER"),
            "password": os.getenv("HANA_PASSWORD"),
            "schema": os.getenv("HANA_SCHEMA")
        }
    return creds
def read_table(schema, table_name, columns="*", where_clause=""):
    query = f'SELECT {columns} FROM "{schema}"."{table_name}" {where_clause}'
    cursor.execute(query)
    cols = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=cols)

def truncate_and_load_entity(table_name: str, columns: list[str], df: pd.DataFrame):
    """
    Svuota (TRUNCATE) e ricarica una tabella CAP
    usando solo le colonne indicate.
    """
    if df.empty:
        logging.warning(f"⚠️ {table_name}: DataFrame vuoto, tabella non aggiornata.")
        return

    col_list = ",".join(f'"{c}"' for c in columns)
    placeholders = ",".join(["?"] * len(columns))

    truncate_sql = f'''
        TRUNCATE TABLE "{SCHEMA_NAME}"."{table_name}"
    '''

    insert_sql = f'''
        INSERT INTO "{SCHEMA_NAME}"."{table_name}"
        ({col_list})
        VALUES ({placeholders})
    '''

    df = df[columns].where(pd.notnull(df), None)

    cursor.execute(truncate_sql)
    cursor.executemany(insert_sql, df.values.tolist())
    conn.commit()

    logging.info(f"✅ {table_name}: caricati {len(df)} record")


# ===========================
# UTILITY TRONCAMENTO TESTI
# ===========================
def truncate_text(value: str | None, max_len: int) -> str | None:
    if value is None:
        return None
    s = str(value)
    if len(s) <= max_len:
        return s
    # Tronco e aggiungo ellissi per segnare il taglio
    return s[:max_len-3] + "..."

MAX_LEN_MATCH_VALUE = 500

# ===========================
# CONNESSIONE HANA
# ===========================
creds = get_hana_credentials()
conn = dbapi.connect(
    address=creds["host"],
    port=int(creds["port"]),
    user=creds["user"],
    password=creds["password"],
    encrypt="true",
    sslValidateCertificate="false"
)
cursor = conn.cursor()
SCHEMA_NAME = creds["schema"]
logging.info("Connessione HANA stabilita.")
# ===========================
# LETTURA TABELLE
# ===========================
tp_company = read_table(SCHEMA_NAME, "DB_TP_COMPANY")
tp_materialtype = read_table(SCHEMA_NAME, "DB_TP_MATERIALTYPE")
tp_caratteristiche = read_table(SCHEMA_NAME, "DB_TP_CARATTERISTICHE")

t001k = read_table(
    SCHEMA_NAME,
    "VT_ECC_T001K",
    columns='"BWKEY","BUKRS"',
    where_clause=f'''
        WHERE BUKRS IN (
            SELECT BUKRS FROM "{SCHEMA_NAME}"."DB_TP_COMPANY"
        )
    '''
)

t001w = read_table(
    SCHEMA_NAME,
    "VT_ECC_T001W",
    columns='"WERKS","NAME1","BWKEY"',
    where_clause=f'''
        WHERE BWKEY IN (
            SELECT BWKEY
            FROM "{SCHEMA_NAME}"."VT_ECC_T001K"
            WHERE BUKRS IN (
                SELECT BUKRS
                FROM "{SCHEMA_NAME}"."DB_TP_COMPANY"
            )
        )
    '''
)

marc = read_table(
    SCHEMA_NAME,
    "VT_ECC_MARC",
    columns='"MATNR","WERKS","LVORM","MMSTA"',
    where_clause=f'''
        WHERE MATNR IN (
            SELECT m."MATNR"
            FROM "{SCHEMA_NAME}"."VT_ECC_MARA" m
            WHERE (m."LVORM" IS NULL OR TRIM(m."LVORM") = '')
        )
        AND WERKS IN (
            SELECT w."WERKS"
            FROM "{SCHEMA_NAME}"."VT_ECC_T001W" w
            INNER JOIN "{SCHEMA_NAME}"."VT_ECC_T001K" k
                ON k."BWKEY" = w."BWKEY"
            INNER JOIN "{SCHEMA_NAME}"."DB_TP_COMPANY" c
                ON c."BUKRS" = k."BUKRS"
        )
    '''
)

mara = read_table(
    SCHEMA_NAME,
    "VT_ECC_MARA",
    columns='"MATNR","ERSDA","ERNAM","LAEDA","AENAM","LVORM","MTART","MATKL","BISMT","MEINS","BSTME","VOLUM","MSTAE","/MATMA/PARTNUMB","/MATMA/LIFNR"',
    where_clause=f'''
        WHERE (LVORM IS NULL OR TRIM(LVORM) = '')
          AND (
              NOT EXISTS (
                  SELECT 1 FROM "{SCHEMA_NAME}"."DB_TP_MATERIALTYPE"
              )
              OR MTART IN (
                  SELECT MTART FROM "{SCHEMA_NAME}"."DB_TP_MATERIALTYPE"
              )
          )
          AND MATNR IN (
              SELECT DISTINCT mc."MATNR"
              FROM "{SCHEMA_NAME}"."VT_ECC_MARC" mc
              INNER JOIN "{SCHEMA_NAME}"."VT_ECC_T001W" w
                  ON w."WERKS" = mc."WERKS"
              INNER JOIN "{SCHEMA_NAME}"."VT_ECC_T001K" k
                  ON k."BWKEY" = w."BWKEY"
              INNER JOIN "{SCHEMA_NAME}"."DB_TP_COMPANY" c
                  ON c."BUKRS" = k."BUKRS"
          )
    '''
)
mara = mara.rename(columns={
    "/MATMA/PARTNUMB": "ZPART_NUM",
    "/MATMA/LIFNR": "LIFNR"
})

makt = read_table(
    SCHEMA_NAME,
    "VT_ECC_MAKT",
    columns='"MATNR","MAKTX","MAKTG"',
    where_clause='''
        WHERE SPRAS = 'IT'
    '''
)
mara = mara.merge(makt,on="MATNR",how="left")

ausp = read_table(
    SCHEMA_NAME,
    "VT_ECC_AUSP",
    columns="""
        SUBSTRING("OBJEK",1,18) AS "MATNR",
        "OBJEK","ATINN","ATZHL","MAFID","KLART","ADZHL",
        "ATWRT","ATFLV","ATAWE","ATFLB","ATAW1","ATCOD",
        "ATTLV","ATTLB","ATPRZ","ATINC","ATAUT","AENNR",
        "DATUV","LKENZ","ATIMB","ATZIS","ATSRT","ATVGLART"
    """,
    where_clause=f'''
        WHERE KLART = '001'
          AND (LKENZ IS NULL OR TRIM(LKENZ) = '')
          AND ATINN IN (
              SELECT ATINN
              FROM "{SCHEMA_NAME}"."DB_TP_CARATTERISTICHE"
          )
          AND SUBSTRING("OBJEK",1,18) IN (
              SELECT MATNR
              FROM "{SCHEMA_NAME}"."VT_ECC_MARA"
              WHERE (LVORM IS NULL OR TRIM(LVORM) = '')
                AND (
                    NOT EXISTS (SELECT 1 FROM "{SCHEMA_NAME}"."DB_TP_MATERIALTYPE")
                    OR MTART IN (
                        SELECT MTART FROM "{SCHEMA_NAME}"."DB_TP_MATERIALTYPE"
                    )
                )
          )
    '''
)

zpn_stor = read_table(
    SCHEMA_NAME,
    "VT_ECC_ZPN_STOR",
    columns='"MATNR","CONT","PARTNUMB","AEDAT","UZEIT","AENAM"',
    where_clause=f'''
        WHERE MATNR IN (
            SELECT MATNR
            FROM "{SCHEMA_NAME}"."VT_ECC_MARA"
            WHERE (LVORM IS NULL OR TRIM(LVORM) = '')
              AND (
                  NOT EXISTS (
                      SELECT 1
                      FROM "{SCHEMA_NAME}"."DB_TP_MATERIALTYPE"
                  )
                  OR MTART IN (
                      SELECT MTART
                      FROM "{SCHEMA_NAME}"."DB_TP_MATERIALTYPE"
                  )
              )
              AND MATNR IN (
                  SELECT DISTINCT m."MATNR"
                  FROM "{SCHEMA_NAME}"."VT_ECC_MARC" m
                  INNER JOIN "{SCHEMA_NAME}"."VT_ECC_T001W" w
                      ON w."WERKS" = m."WERKS"
                  INNER JOIN "{SCHEMA_NAME}"."VT_ECC_T001K" k
                      ON k."BWKEY" = w."BWKEY"
                  INNER JOIN "{SCHEMA_NAME}"."DB_TP_COMPANY" c
                      ON c."BUKRS" = k."BUKRS"
              )
        )
    '''
)

zmm_mat_text = read_table(
    SCHEMA_NAME,
    "VT_ECC_ZMM_MAT_TEXT",
    columns='"MATNR","TDID","LINE_ID","TDLINE"',
    where_clause=f'''
        WHERE MATNR IN (
            SELECT MATNR
            FROM "{SCHEMA_NAME}"."VT_ECC_MARA"
            WHERE (LVORM IS NULL OR TRIM(LVORM) = '')
              AND (
                  NOT EXISTS (
                      SELECT 1
                      FROM "{SCHEMA_NAME}"."DB_TP_MATERIALTYPE"
                  )
                  OR MTART IN (
                      SELECT MTART
                      FROM "{SCHEMA_NAME}"."DB_TP_MATERIALTYPE"
                  )
              )
              AND MATNR IN (
                  SELECT DISTINCT m."MATNR"
                  FROM "{SCHEMA_NAME}"."VT_ECC_MARC" m
                  INNER JOIN "{SCHEMA_NAME}"."VT_ECC_T001W" w
                      ON w."WERKS" = m."WERKS"
                  INNER JOIN "{SCHEMA_NAME}"."VT_ECC_T001K" k
                      ON k."BWKEY" = w."BWKEY"
                  INNER JOIN "{SCHEMA_NAME}"."DB_TP_COMPANY" c
                      ON c."BUKRS" = k."BUKRS"
              )
        )
    '''
)

zmm_mat_text = zmm_mat_text[zmm_mat_text["TDLINE"].notna()&(zmm_mat_text["TDLINE"].str.strip() != "")].copy()
zmm_mat_text["LINE_ID"] = zmm_mat_text["LINE_ID"].astype(int)
zmm_mat_text = zmm_mat_text.sort_values(
    ["MATNR", "TDID", "LINE_ID"]
)
testimat = (
    zmm_mat_text
    .groupby(["MATNR", "TDID"], as_index=False)
    .agg({
        "TDLINE": lambda x: "\n".join(x)
    })
    .rename(columns={
        "TDLINE": "ZESTESO",
        "TDID": "ZTESTO"
    })
)
testimat["ZESTESO"] = testimat["ZESTESO"].str.slice(0, 1000)
testimat = testimat.drop_duplicates(subset=["MATNR", "ZTESTO"])


lfa1 = read_table(
    SCHEMA_NAME,
    "VT_ECC_LFA1",
    columns='"LIFNR","LAND1","NAME1"',
    where_clause=f'''
        WHERE LIFNR IN (
            SELECT DISTINCT m."/MATMA/LIFNR"
            FROM "{SCHEMA_NAME}"."VT_ECC_MARA" m
            INNER JOIN "{SCHEMA_NAME}"."VT_ECC_MARC" mc
                ON mc."MATNR" = m."MATNR"
            INNER JOIN "{SCHEMA_NAME}"."VT_ECC_T001W" w
                ON w."WERKS" = mc."WERKS"
            INNER JOIN "{SCHEMA_NAME}"."VT_ECC_T001K" k
                ON k."BWKEY" = w."BWKEY"
            INNER JOIN "{SCHEMA_NAME}"."DB_TP_COMPANY" c
                ON c."BUKRS" = k."BUKRS"
            WHERE (m."LVORM" IS NULL OR TRIM(m."LVORM") = '')
              AND (
                  NOT EXISTS (
                      SELECT 1
                      FROM "{SCHEMA_NAME}"."DB_TP_MATERIALTYPE"
                  )
                  OR m."MTART" IN (
                      SELECT MTART
                      FROM "{SCHEMA_NAME}"."DB_TP_MATERIALTYPE"
                  )
              )
        )
    '''
)

mchb = read_table(
    SCHEMA_NAME,
    "VT_ECC_MCHB",
    columns='"MATNR","WERKS","LGORT","CHARG","CLABS"',
    where_clause=f'''
        WHERE CLABS <> 0
          AND (MATNR, WERKS) IN (
              SELECT DISTINCT m."MATNR", mc."WERKS"
              FROM "{SCHEMA_NAME}"."VT_ECC_MARA" m
              INNER JOIN "{SCHEMA_NAME}"."VT_ECC_MARC" mc
                  ON mc."MATNR" = m."MATNR"
              INNER JOIN "{SCHEMA_NAME}"."VT_ECC_T001W" w
                  ON w."WERKS" = mc."WERKS"
              INNER JOIN "{SCHEMA_NAME}"."VT_ECC_T001K" k
                  ON k."BWKEY" = w."BWKEY"
              INNER JOIN "{SCHEMA_NAME}"."DB_TP_COMPANY" c
                  ON c."BUKRS" = k."BUKRS"
              WHERE (m."LVORM" IS NULL OR TRIM(m."LVORM") = '')
                AND (
                    NOT EXISTS (
                        SELECT 1
                        FROM "{SCHEMA_NAME}"."DB_TP_MATERIALTYPE"
                    )
                    OR m."MTART" IN (
                        SELECT MTART
                        FROM "{SCHEMA_NAME}"."DB_TP_MATERIALTYPE"
                    )
                )
          )
    '''
)

filtro_aedat = date.today().replace(year=date.today().year - 10)
filtro_aedat_str = filtro_aedat.strftime("%Y%m%d")

ekko = read_table(
    SCHEMA_NAME,
    "VT_ECC_EKKO",
    columns='"EBELN","BUKRS","BSTYP","BSART","AEDAT","ERNAM","LIFNR","KDATB","KDATE","ZZATT","ZZFINE","FRGKE","WAERS"',
    where_clause=f'''
        WHERE (LOEKZ IS NULL OR TRIM(LOEKZ) = '')
          AND BUKRS IN (
            SELECT BUKRS
            FROM "{SCHEMA_NAME}"."DB_TP_COMPANY" )
          AND BSART IN ( 'ODCV','ZEPR','ODAM','ZCCC','ZCCV','ZCCM','ZQAM','ZWKS' )  
          AND AEDAT >= '{filtro_aedat_str}'
          AND FRGKE IN ('B','S','')
    '''
)

ekpo = read_table(
    SCHEMA_NAME,
    "VT_ECC_EKPO",
    columns='''
        "EBELN","EBELP","LOEKZ","STATU","AEDAT","TXZ01",
        "MATNR","BUKRS","WERKS","LGORT",
        "MENGE","MEINS","NETPR","PEINH","NETWR","KONNR"
    ''',
    where_clause=f'''
        WHERE (LOEKZ IS NULL OR TRIM(LOEKZ) = '')
          AND EBELN IN (
              SELECT k."EBELN"
              FROM "{SCHEMA_NAME}"."VT_ECC_EKKO" k
              WHERE k."BUKRS" IN (
                    SELECT BUKRS
                    FROM "{SCHEMA_NAME}"."DB_TP_COMPANY")
                AND k."BSART" IN ( 'ODCV','ZEPR','ODAM','ZCCC','ZCCV','ZCCM','ZQAM','ZWKS' )  
                AND k."AEDAT" >= '{filtro_aedat_str}'
                AND k."FRGKE" IN ('B','S','')
          )
          AND MATNR IN (
              SELECT MATNR
              FROM "{SCHEMA_NAME}"."DB_MARA"
          )
    '''
)

#Eventuale filtro su Tipo Materiale
if tp_materialtype.empty:
    # fallback: tutti i MTART presenti nella MARA già filtrata
    valid_mtart = (mara["MTART"].dropna().astype(str).unique().tolist())
else:
    valid_mtart = (tp_materialtype["MTART"].dropna().astype(str).unique().tolist())

# ===========================
# Caricamento Tabelle
# ===========================

truncate_and_load_entity(
    table_name="DB_T001K",
    columns=["BWKEY", "BUKRS"],
    df=t001k
)

truncate_and_load_entity(
    table_name="DB_T001W",
    columns=["WERKS", "NAME1", "BWKEY"],
    df=t001w
)

truncate_and_load_entity(
    table_name="DB_MARC",
    columns=["MATNR", "WERKS", "LVORM", "MMSTA"],
    df=marc
)

truncate_and_load_entity(
    table_name="DB_MARA",
    columns=["MATNR","ERSDA","ERNAM","LAEDA","AENAM","LVORM","MTART","MATKL","BISMT","MEINS","BSTME",
             "VOLUM","MSTAE","ZPART_NUM","LIFNR","MAKTX","MAKTG"],
    df=mara
)

truncate_and_load_entity(
    table_name="DB_AUSP",
    columns=["MATNR","OBJEK","ATINN","ATZHL","MAFID","KLART","ADZHL","ATWRT","ATFLV","ATAWE","ATFLB","ATAW1","ATCOD",
             "ATTLV","ATTLB","ATPRZ","ATINC","ATAUT","AENNR","DATUV","LKENZ","ATIMB","ATZIS","ATSRT","ATVGLART"],
    df=ausp
)

truncate_and_load_entity(
    table_name="DB_ZPN_STOR",
    columns=["MATNR", "CONT", "PARTNUMB", "AEDAT", "UZEIT", "AENAM"],
    df=zpn_stor
)

truncate_and_load_entity(
    table_name="DB_TESTIMAT",
    columns=["MATNR", "ZTESTO", "ZESTESO"],
    df=testimat
)

truncate_and_load_entity(
    table_name="DB_LFA1",
    columns=["LIFNR","LAND1","NAME1"],
    df=lfa1
)

truncate_and_load_entity(
    table_name="DB_MCHB",
    columns=["MATNR","WERKS","LGORT","CHARG","CLABS"],
    df=mchb
)

truncate_and_load_entity(
    table_name="DB_EKKO",
    columns=["EBELN","BUKRS","BSTYP","BSART","AEDAT","ERNAM","LIFNR","KDATB","KDATE","ZZATT","ZZFINE","FRGKE","WAERS"],
    df=ekko
)

truncate_and_load_entity(
    table_name="DB_EKPO",
    columns=["EBELN","EBELP","LOEKZ","STATU","AEDAT","TXZ01","MATNR","BUKRS","WERKS","LGORT",
             "MENGE","MEINS","NETPR","PEINH","NETWR","KONNR"],
    df=ekpo
)



all_results = []

for mtart in valid_mtart:
    logging.info(f"▶ Elaborazione per MTART: {mtart}")

# Filtra materiali per MTART corrente
    mara_filtered = mara[mara["MTART"] == mtart].copy()
    if mara_filtered.empty:
        continue
# Filtra anche le altre tabelle in base ai materiali del gruppo
    mara_filtered.sort_values(by=["ERSDA"], ascending=False, inplace=True) ### ordino per data di creazione così il materiale principale è il più recente
    testimat = testimat[testimat['MATNR'].isin(mara_filtered['MATNR'])].copy()
    zpn_stor = zpn_stor[zpn_stor['MATNR'].isin(mara_filtered['MATNR'])].copy()

# caratteristiche ordinata
    tp_caratteristiche_sorted = tp_caratteristiche.sort_values(by="PRIORITY", ascending=True)
    target_codes = tp_caratteristiche_sorted["ATINN"].astype(str).tolist()

# ausp filtro
    ausp_joined = ausp[ausp["OBJEK"].isin(mara_filtered['MATNR'])].copy()

# report
    logging.info(f"Righe AUSP iniziali: {len(ausp)}")
    logging.info(f"Righe finali dopo join con MARA: {len(ausp_joined)}")
    logging.info(f"Materiali filtrati: {len(mara_filtered)}")

# ===========================
# VARIABILI GLOBALI
# ===========================
    results = []
    processed = set()
    pairs_added = set()

    MIN_PN_LENGTH = 5

# ===========================
# FUNZIONE PER AGGIUNGERE MATCH UNICO
# ===========================
    def add_match(driver, duplicate, score, criterio, match_value):
        
        if (driver, duplicate) not in pairs_added:
            results.append({
                "MATNR": driver,
                "MATNRD": duplicate,
                "MATCH_SCORE": score,
                "CRITERIO": criterio,
                "MATCH_VALUE": truncate_text(match_value, MAX_LEN_MATCH_VALUE)
            })
            pairs_added.add((driver, duplicate))
            processed.add(driver)
            processed.add(duplicate)
    
    lifnr_by_matnr = mara_filtered.set_index("MATNR")["LIFNR"].to_dict()
    def get_lifnr(matnr):
        return lifnr_by_matnr.get(matnr)

    def supplier_lifnr(driver_lifnr, match_lifnr, score_if_same, score_if_diff):
        if driver_lifnr and match_lifnr and str(driver_lifnr).strip() == str(match_lifnr).strip():
            return score_if_same
        return score_if_diff

    # ==========================================
    # STEP 1 - Materiali con ZPART_NUM valorizzato
    # ==========================================
    drivers = mara_filtered[mara_filtered["ZPART_NUM"].notna()]
    for _, driver in drivers.iterrows():
        matnr_driver = driver["MATNR"]
        if matnr_driver in processed:
            continue
        part_num = str(driver["ZPART_NUM"]).strip()
        supplier = driver["LIFNR"]
        if not part_num or len(part_num) < MIN_PN_LENGTH:
            continue
        pattern = re.escape(part_num)
    
        # --- 1. Match stesso P/N
        matches_pn = mara_filtered[mara_filtered["ZPART_NUM"] == part_num]
        for _, match in matches_pn.iterrows():
            if match["MATNR"] != matnr_driver and match["MATNR"] not in processed:
                score = supplier_lifnr(
                driver["LIFNR"],
                match["LIFNR"],
                100,   # stesso fornitore
                90     # fornitore diverso o mancante
            )
                criterio = (
                "1.1a) {P/N + Fornitore} uguali"
                if score == 100
                else "1.1b) P/N uguale, ma fornitore differente"
            )
                add_match(matnr_driver, match["MATNR"], score, criterio, f"{part_num}" ' -> ' f"{match["ZPART_NUM"]}" )

        # --- 2. Match 90% storico
        storico_matches = zpn_stor[zpn_stor["PARTNUMB"].str.strip() == part_num]
        for _, match in storico_matches.iterrows():
            matnr_storico = str(match["MATNR"])
            if matnr_storico != matnr_driver and matnr_storico not in processed:
                score = supplier_lifnr(
                driver["LIFNR"],
                get_lifnr(matnr_storico),
                100,   # stesso fornitore
                90     # fornitore diverso o mancante
            )
                criterio = (
                "1.2a) {P/N + Fornitore}, presente nello storico P/N di un altro materiale"
                if score == 100
                else "1.2b) P/N presente nello storico P/N di un altro materiale"
            )
                add_match(matnr_driver, matnr_storico, score, criterio, f"{part_num}" ' -> ' f"{match["PARTNUMB"]}" )

        # --- 3. Match 90% AUSP
        ausp_matches = ausp_joined[(ausp_joined["ATWRT"].str.contains(pattern, case=False, na=False, regex=True)) &
                            (ausp_joined["ATINN"].isin(target_codes))]
        for _, match in ausp_matches.iterrows():
            objek_matnr = str(match["OBJEK"][:18])
            if objek_matnr != matnr_driver and objek_matnr not in processed:
                score = supplier_lifnr(
                driver["LIFNR"],
                get_lifnr(objek_matnr),
                100,   # stesso fornitore
                90     # fornitore diverso o mancante
            )
                criterio = (
                "1.3a) {P/N + Fornitore}, presente nelle caratteristiche di classificazione di un altro materiale"
                if score == 100
                else "1.3b) P/N presente nelle caratteristiche di classificazione di un altro materiale"
            )
                add_match(matnr_driver, objek_matnr, score, criterio, f"{part_num}" ' -> ' f"{match["ATWRT"]}" )

        # --- 4. Match 80% descrizioni
        desc_matches = mara_filtered[mara_filtered["MAKTX"].str.contains(pattern, case=False, na=False, regex=True) |
                                    mara_filtered["MAKTG"].str.contains(pattern, case=False, na=False, regex=True)]
        for _, match in desc_matches.iterrows():
            if match["MATNR"] != matnr_driver and match["MATNR"] not in processed:
                add_match(matnr_driver, match["MATNR"], 80, "1.4) P/N presente nella Descrizione breve di un altro materiale", f"{part_num}" ' -> ' f"{match["MAKTG"]}" )

        # --- 5. Match 81% testi estesi
        testi_matches = testimat[testimat["ZESTESO"].str.contains(pattern, case=False, na=False, regex=True)]
        for _, match in testi_matches.iterrows():
            matnr_testo = match["MATNR"]
            if matnr_testo != matnr_driver and matnr_testo not in processed:
                add_match(matnr_driver, matnr_testo, 80, "1.5) P/N presente nei Testi estesi di un altro materiale", f"{part_num}" ' -> ' f"{match["ZESTESO"]}"  )

    logging.info(f"- Step 1: Materiali processati con ZPART_NUM:      {len(processed)}")

    # ============================================================== 
    # STEP 2 - Materiali senza ZPART_NUM - P/N dallo storico 
    # ==============================================================
    mara_blank = mara_filtered[(mara_filtered["ZPART_NUM"].isna() | (mara_filtered["ZPART_NUM"].str.strip() == ""))]
    matched_step2 = 0
    for _, material in mara_blank.iterrows():
        matnr_blank = material["MATNR"]
        if matnr_blank in processed:
            continue
        storico_matches = zpn_stor[zpn_stor["MATNR"].astype(str).str.strip() == str(matnr_blank).strip()]
        if storico_matches.empty:
            continue
        storico_matches = storico_matches.sort_values(by=["AEDAT", "UZEIT"], ascending=False)
        driver_partnum = str(storico_matches.iloc[0]["PARTNUMB"]).strip()
        if not driver_partnum or len(driver_partnum) < MIN_PN_LENGTH:
            continue
        pattern = re.escape(driver_partnum)
        # --- 1. MATCH 90%: materiali con lo stesso P/N nello storico
        same_pn_matches = zpn_stor[zpn_stor["PARTNUMB"].str.strip() == driver_partnum] 
        for _, match in same_pn_matches.iterrows():
            matnr_match = str(match["MATNR"])
            if matnr_match != matnr_blank and matnr_match not in processed:
                score = supplier_lifnr(
                get_lifnr(matnr_blank),
                get_lifnr(matnr_match),
                100,   # stesso fornitore
                90     # fornitore diverso o mancante
            )
                criterio = (
                "2.1.1a) {P/N storico + Fornitore}, uguali"
                if score == 100
                else "2.1.1b) P/N storico uguali"
            )
                add_match(matnr_blank, matnr_match, score, criterio, f"{driver_partnum}" ' -> ' f"{match["PARTNUMB"]}" )

        # --- 2. MATCH 90%: AUSP
        ausp_matches = ausp_joined[(ausp_joined["ATWRT"].str.contains(pattern, case=False, na=False, regex=True)) &
                            (ausp_joined["ATINN"].isin(target_codes))]
        for _, match in ausp_matches.iterrows():
            objek_matnr = str(match["OBJEK"][:18])
            if objek_matnr != matnr_blank and objek_matnr not in processed:
                score = supplier_lifnr(
                get_lifnr(matnr_blank),
                get_lifnr(objek_matnr),
                100,   # stesso fornitore
                90     # fornitore diverso o mancante
            )
                criterio = (
                "2.1.2a) {P/N storico + Fornitore}, presente nelle caratteristiche di classificazione di un altro materiale"
                if score == 100
                else "2.1.2b) P/N storico presente nelle caratteristiche di classificazione di un altro materiale"
            )
                add_match(matnr_blank, objek_matnr, score, criterio, f"{driver_partnum}" ' -> ' f"{match["ATWRT"]}")

        # --- 3. MATCH 80%: descrizioni brevi
        desc_matches = mara_filtered[mara_filtered["MAKTX"].str.contains(pattern, case=False, na=False, regex=True) |
                                    mara_filtered["MAKTG"].str.contains(pattern, case=False, na=False, regex=True)]
        for _, match in desc_matches.iterrows():
            if match["MATNR"] != matnr_blank and match["MATNR"] not in processed:
                add_match(matnr_blank, match["MATNR"], 80, "2.1.3) P/N storico presente nella Descrizione breve di un altro materiale", f"{driver_partnum}" ' -> ' f"{match["MAKTG"]}" )

        # --- 4. MATCH 84%: testi estesi
        testi_matches = testimat[testimat["ZESTESO"].str.contains(pattern, case=False, na=False, regex=True)]
        for _, match in testi_matches.iterrows():
            matnr_testo = match["MATNR"]
            if matnr_testo != matnr_blank and matnr_testo not in processed:
                add_match(matnr_blank, matnr_testo, 80, "2.1.4) P/N storico presente nei Testi estesi di un altro materiale",f"{driver_partnum}" ' -> ' f"{match["ZESTESO"]}")
  
        matched_step2 += 1

    logging.info(f"- Step 2: Materiali processati con PN storico:      {len(processed)}")

    # ============================================================== 
    # STEP 3 - Materiali senza ZPART_NUM - P/N dalle caratteristiche
    # ==============================================================
    mara_blank2 = mara_blank[~mara_blank["MATNR"].isin(processed)]
    ausp_joined.loc[:, "MATNR"] = ausp_joined["OBJEK"].astype(str).str[:18]
    ausp_sorted = ausp_joined.merge(tp_caratteristiche_sorted, on="ATINN", how="left")
    ausp_sorted = ausp_sorted.sort_values(by=["MATNR", "PRIORITY"])
    first_char = ausp_sorted.groupby("MATNR").first().reset_index()[["MATNR", "ATWRT"]]
    step3_df = mara_blank2.merge(first_char, on="MATNR", how="left").dropna(subset=["ATWRT"])
    step3_matches = 0
    for _, row in step3_df.iterrows():
        matnr_remain = row["MATNR"]
        if matnr_remain in processed:
            continue
        pn_driver = str(row["ATWRT"]).strip()
        if not pn_driver or len(pn_driver) < MIN_PN_LENGTH:
            continue
        pattern = re.escape(pn_driver)
        # --- 1. Match in AUSP
        ausp_matches = ausp_joined[ausp_joined["ATWRT"].str.contains(pattern, case=False, na=False, regex=True)]
        for _, match in ausp_matches.iterrows():
            objek_matnr = match["MATNR"]
            if objek_matnr != matnr_remain and objek_matnr not in processed:
                score = supplier_lifnr(
                get_lifnr(matnr_remain),
                get_lifnr(objek_matnr),
                100,   # stesso fornitore
                90     # fornitore diverso o mancante
            )
                criterio = (
                "2.2.1a) {P/N + Fornitore} uguali, presenti nelle Caratteristiche di classificazione di materiali differenti"
                if score == 100
                else "2.2.1b) P/N uguali presenti nelle Caratteristiche di classificazione di materiali differenti"
            )
                add_match(matnr_remain, objek_matnr, score, criterio, f"{pn_driver}" ' -> ' f"{match['ATWRT']}" )

        # --- 2. Match nelle descrizioni brevi
        desc_matches = mara_filtered[mara_filtered["MAKTX"].str.contains(pattern, case=False, na=False, regex=True) |
                                    mara_filtered["MAKTG"].str.contains(pattern, case=False, na=False, regex=True)]
        for _, match in desc_matches.iterrows():
            if match["MATNR"] != matnr_remain and match["MATNR"] not in processed:
                add_match(matnr_remain, match["MATNR"], 80, "2.2.2) P/N delle Caratteristiche di classificazione presente in Descrizione Breve di un altro materiale", f"{pn_driver}" ' -> ' f"{match["MAKTG"]}" )

        # --- 3. Match nei testi estesi
        testi_matches = testimat[testimat["ZESTESO"].str.contains(pattern, case=False, na=False, regex=True)]
        for _, match in testi_matches.iterrows():
            matnr_testo = match["MATNR"]
            if matnr_testo != matnr_remain and matnr_testo not in processed:
                add_match(matnr_remain, matnr_testo, 80, "2.2.3) P/N delle Caratteristiche di classificazione presente nei Testi estesi di un altro materiale", f"{pn_driver}" ' -> ' f"{match["ZESTESO"]}" )

        step3_matches += 1

    logging.info(f"- Step 3: Materiali processati con PN Caratteristiche:      {len(processed)}")


    # ===========================
    # STEP 4 - FUZZY SEARCH OTTIMIZZATO (SCALABILE)
    # ===========================

    drivers_fuzzy = mara_filtered[~mara_filtered["MATNR"].isin(processed)].copy()

    drivers_fuzzy["SHORT_TEXT"] = drivers_fuzzy["MAKTG"].fillna("").astype(str)

    # --- Estrazione testi estesi
    testi_grun = (
        testimat.assign(ZTESTO_UP=testimat["ZTESTO"].astype(str).str.upper())
        .loc[lambda df: df["ZTESTO_UP"] == "GRUN", ["MATNR", "ZESTESO"]]
        .groupby("MATNR", as_index=False)
        .first()
        .rename(columns={"ZESTESO": "LONG_TEXT_BASE"})
    )

    testi_best = (
        testimat.assign(ZTESTO_UP=testimat["ZTESTO"].astype(str).str.upper())
        .loc[lambda df: df["ZTESTO_UP"] == "BEST", ["MATNR", "ZESTESO"]]
        .groupby("MATNR", as_index=False)
        .first()
        .rename(columns={"ZESTESO": "LONG_TEXT_ACQ"})
    )

    drivers_fuzzy = drivers_fuzzy.merge(testi_grun, on="MATNR", how="left")
    drivers_fuzzy = drivers_fuzzy.merge(testi_best, on="MATNR", how="left")

    # ===========================
    # PARAMETRI FUZZY
    # ===========================
    MIN_TOKENS_FUZZY = 2

    SHORT_TEXT_THRESHOLD = 0.80
    LONG_TEXT_THRESHOLD  = 0.80

    FUZZY_SCORE_MAKTG = 30
    FUZZY_SCORE_GRUN  = 40
    FUZZY_SCORE_BEST  = 40

    # ===========================
    # NORMALIZZAZIONE / TOKEN
    # ===========================
    _PUNCT_RE = re.compile(r"[^\w\s/\-_.+\(\)°Ω±]+")

    def normalize_text(s):
        if s is None or (isinstance(s, float) and math.isnan(s)):
            return ""
        s = str(s).lower()
        s = _PUNCT_RE.sub(" ", s)
        return re.sub(r"\s{2,}", " ", s).strip()

    def tokens(s):
        return [t for t in s.split() if len(t) >= 3]

    def trigrams(s):
        s = s.replace(" ", "_")
        if len(s) < 3:
            return {s}
        return {s[i:i+3] for i in range(len(s)-2)}

    def jaccard(a, b):
        if not a or not b:
            return 0.0
        return len(a & b) / len(a | b)

    def seq_ratio(a, b):
        return difflib.SequenceMatcher(None, a, b).ratio()

    # ===========================
    # PRECOMPUTE + BLOCKING
    # ===========================
    fuzzy_cache = {}
    blocks = defaultdict(set)

    for r in drivers_fuzzy.itertuples(index=False):
        matnr = r.MATNR

        sn = normalize_text(r.SHORT_TEXT)
        st = set(tokens(sn))
        sg = trigrams(sn)

        bn = normalize_text(r.LONG_TEXT_BASE)
        bt = set(tokens(bn))
        bg = trigrams(bn)

        an = normalize_text(r.LONG_TEXT_ACQ)
        at = set(tokens(an))
        ag = trigrams(an)

        fuzzy_cache[matnr] = {
            "short": (sn, st, sg),
            "base":  (bn, bt, bg),
            "acq":   (an, at, ag)
        }

        if st:
            blocks[next(iter(st))].add(matnr)

    # ===========================
    # RECENCY
    # ===========================
    order = mara_filtered[["MATNR"]].reset_index(drop=True)
    RECENCY_RANK = {m: i for i, m in enumerate(order["MATNR"])}

    def most_recent(group):
        return min(group, key=lambda x: RECENCY_RANK.get(x, 1e9))

    # ===========================
    # SIMILARITÀ COMPOSITA
    # ===========================
    def composite_similarity(a, b, threshold):
        tok_sim = jaccard(a[1], b[1])
        if tok_sim == 0:
            return 0.0

        tri_sim = jaccard(a[2], b[2])
        upper = 0.5 + 0.3 * tok_sim + 0.2 * tri_sim
        if upper < threshold:
            return 0.0

        return 0.5 * seq_ratio(a[0][:3000], b[0][:3000]) + 0.3 * tok_sim + 0.2 * tri_sim

    # ===========================
    # FUNZIONE FUZZY GENERICA
    # ===========================
    def fuzzy_phase(field, threshold, score, criterio):
        for dm in list(fuzzy_cache.keys()):
            if dm in processed:
                continue

            a = fuzzy_cache[dm][field]
            if len(a[1]) < MIN_TOKENS_FUZZY:
                continue

            block_key = next(iter(a[1]))
            candidates = blocks.get(block_key, fuzzy_cache.keys())

            dups = []
            for c in candidates:
                if c == dm or c in processed:
                    continue
                b = fuzzy_cache[c][field]
                if len(b[1]) < MIN_TOKENS_FUZZY:
                    continue

                if composite_similarity(a, b, threshold) >= threshold:
                    dups.append(c)

            if dups:
                group = [dm] + dups
                driver = most_recent(group)
                for x in group:
                    if x != driver:
                        add_match(driver, x, score, criterio, " ")

    # ===========================
    # ESECUZIONE FUZZY
    # ===========================
    fuzzy_phase("short", SHORT_TEXT_THRESHOLD, FUZZY_SCORE_MAKTG,
                "3.1) Corrispondenza alta tra descrizioni brevi")

    fuzzy_phase("base", LONG_TEXT_THRESHOLD, FUZZY_SCORE_GRUN,
                "3.2) Corrispondenza alta tra testi estesi - base")

    fuzzy_phase("acq", LONG_TEXT_THRESHOLD, FUZZY_SCORE_BEST,
                "3.2) Corrispondenza alta tra testi estesi - acquisti")

    # --- Riepilogo fuzzy
    FUZZY_SCORES = {FUZZY_SCORE_MAKTG, FUZZY_SCORE_GRUN, FUZZY_SCORE_BEST}
    df_fuzzy = pd.DataFrame(
        [r for r in results if r.get("MATCH_SCORE") in FUZZY_SCORES]
    ) if results else pd.DataFrame(columns=["MATNR","MATNRD", "MATCH_SCORE", "CRITERIO","MATCH_VALUE"])
    print("🧪  STEP 4 - Fuzzy completato")
    print(f"- Match fuzzy aggiunti: {len(df_fuzzy)}")
        # ===========================
    fuzzy_count = len(df_fuzzy) if 'df_fuzzy' in globals() else 0
    print("🧪  STEP 4 - Fuzzy search su testi")
    print(f"- Match fuzzy aggiunti: {fuzzy_count}")
    if fuzzy_count > 0:
        print("  • Top esempi:")
        print(df_fuzzy.head(5)[["MATNR","MATNRD","MATCH_SCORE","CRITERIO"]].to_string(index=False))

    materiali_senza_match_step4 = len(drivers_fuzzy) - fuzzy_count
    # ==============================================================
    # STEP 5 - MATCH SU PART NUMBER CORTI (POST-FUZZY)
    # ==============================================================
    # Materiali rimasti da processare dopo Step 4
    remaining_materials = mara_filtered[~mara_filtered["MATNR"].isin(processed)].copy()
    # AUSP limitata ai materiali rimasti
    ausp_remaining = ausp_joined[ausp_joined["MATNR"].isin(remaining_materials["MATNR"])].copy()

    # Filtra solo materiali con PN corto
    drivers_short_pn = remaining_materials[
    remaining_materials["ZPART_NUM"].notna() &
    (remaining_materials["ZPART_NUM"].str.strip() != "") &
    (remaining_materials["ZPART_NUM"].str.len() < MIN_PN_LENGTH)].copy()

    logging.info(f"- Step 5: Materiali con PN corto da analizzare: {len(drivers_short_pn)}")

    for _, driver in drivers_short_pn.iterrows():
        matnr_driver = driver["MATNR"]
        if matnr_driver in processed:
            continue

        pn_short = driver["ZPART_NUM"].strip()
        lifnr_driver = driver["LIFNR"]

        # --- 5.1 Match diretto su MARA (PN corto ESATTO)
        same_pn_matches = remaining_materials[
            remaining_materials["ZPART_NUM"].str.strip() == pn_short
        ]

        for _, match in same_pn_matches.iterrows():
            matnr_match = match["MATNR"]
            if matnr_match == matnr_driver or matnr_match in processed:
                continue

            score = supplier_lifnr(
                lifnr_driver,
                match["LIFNR"],
                10,
                5
            )

            criterio = (
                "4.1a) PN corto + Fornitore uguali"
                if score == 10
                else "4.1b) PN corto uguale, fornitore differente"
            )

            add_match(
                matnr_driver,
                matnr_match,
                score,
                criterio,
                pn_short
            )

        # --- 5.2 Match su storico PN (ESATTO)
        storico_matches = zpn_stor[
            zpn_stor["PARTNUMB"].str.strip() == pn_short
        ]

        for _, match in storico_matches.iterrows():
            matnr_storico = str(match["MATNR"])
            if matnr_storico == matnr_driver or matnr_storico in processed:
                continue

            score = supplier_lifnr(
                lifnr_driver,
                get_lifnr(matnr_storico),
                10,
                5
            )

            criterio = (
                "4.2a) PN storico corto + Fornitore uguali"
                if score == 10
                else "4.2b) PN storico corto uguale"
            )

            add_match(
                matnr_driver,
                matnr_storico,
                score,
                criterio,
                pn_short
            )
   
    # --- 5.3 Match PN corto in AUSP
        ausp_matches = ausp_remaining[
        ausp_remaining["ATWRT"].str.strip() == pn_short]


        for _, match in ausp_matches.iterrows():
            matnr_ausp = str(match["MATNR"])
            if matnr_ausp == matnr_driver or matnr_ausp in processed:
                continue

            score = supplier_lifnr(
                lifnr_driver,
                get_lifnr(matnr_ausp),
                10,
                5)

            criterio = ( "4.3a) PN corto + Fornitore uguali nelle caratteristiche"
                if score == 10
                    else "4.3b) PN corto uguale nelle caratteristiche" )

            add_match(
                matnr_driver,
                matnr_ausp,
                score,
                criterio,
                pn_short
                )

    logging.info(f"- Step 5: Materiali processati con PN corto: {len(processed)}")


    # ===========================
    # AGGIUNTA INFORMAZIONI EXTRA
    # ===========================
    df_results = pd.DataFrame(
    results,
    columns=["MATNR", "MATNRD", "MATCH_SCORE", "CRITERIO", "MATCH_VALUE"])

    if df_results.empty:
        logging.warning(f"⚠️ MTART {mtart}: nessun match trovato.")
    else:
        df_results["INSERT_DATE"] = date.today().isoformat()
        all_results.append(df_results)

    # ===========================
    # SCRITTURA RISULTATI IN HANA
    # ===========================
if not all_results:
    logging.warning("⚠️ Nessun match trovato per nessun MTART. Tabella non aggiornata.")
else:
    final_df = pd.concat(all_results, ignore_index=True)

    cursor.execute(f'''
        TRUNCATE TABLE "{SCHEMA_NAME}"."DB_DUPLICATED_MATERIAL"
    ''')

    insert_query = f'''
        INSERT INTO "{SCHEMA_NAME}"."DB_DUPLICATED_MATERIAL"
        ("MATNR", "MATNRD", "MATCH_SCORE", "CRITERIO", "MATCH_VALUE", "INSERT_DATE")
        VALUES (?, ?, ?, ?, ?, ?)
    '''

    final_df = final_df.where(pd.notnull(final_df), None)
    cursor.executemany(
        insert_query,
        final_df[[
            "MATNR", "MATNRD", "MATCH_SCORE",
            "CRITERIO", "MATCH_VALUE", "INSERT_DATE"
        ]].values.tolist()
    )

    conn.commit()

    logging.info(f"✅ Inseriti {len(final_df)} match totali su DB_DUPLICATED_MATERIAL")

    # ===========================
    # RIEPILOGO FINALE
    # ===========================
    elapsed_sec = time.time() - start_time
    hours = int(elapsed_sec // 3600)
    minutes = int((elapsed_sec % 3600) // 60)
    seconds = int(elapsed_sec % 60)
    elapsed_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    total_matches = len(final_df)
    unique_drivers = final_df["MATNR"].nunique()
    unique_duplicates = final_df["MATNRD"].nunique()
    involved_materials = set(final_df["MATNR"]).union(set(final_df["MATNRD"]))

    # Distribuzione per score
    score_distribution = (
        final_df["MATCH_SCORE"]
        .value_counts()
        .sort_index()
    )

    # Distribuzione per criterio (top 10)
    criterio_distribution = (
        final_df["CRITERIO"]
        .value_counts()
        .head(10)
    )

    print("\n" + "=" * 60)
    print("📊 RIEPILOGO FINALE ELABORAZIONE DUPLICATI MATERIALI")
    print("=" * 60 + "\n")

    print("📦 RISULTATI COMPLESSIVI")
    print(f"- Match totali individuati:              {total_matches}")
    print(f"- Driver unici:                           {unique_drivers}")
    print(f"- Duplicati unici:                       {unique_duplicates}")
    print(f"- Materiali complessivamente coinvolti:  {len(involved_materials)}\n")

    print("🧮 DISTRIBUZIONE MATCH PER SCORE")
    for score, cnt in score_distribution.items():
        print(f"- Score {score:>3}: {cnt}")
    print()

    print("🧠 TOP 10 CRITERI DI MATCH")
    for crit, cnt in criterio_distribution.items():
        print(f"- {crit}: {cnt}")
    print()

    print("🗂️ DATABASE")
    print(f"- Tabella DB_DUPLICATED_MATERIAL aggiornata")
    print(f"- Record scritti: {len(final_df)}\n")

    print("⏱️ DURATA ELABORAZIONE")
    print(f"- Tempo totale: {elapsed_str}\n")

    print("=" * 60)
    print("✅ ELABORAZIONE COMPLETATA CON SUCCESSO")
    print("=" * 60 + "\n")