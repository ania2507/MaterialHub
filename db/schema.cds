namespace db;

entity T001K {
    key BWKEY : String(4);    // Area valorizzazione
    BUKRS     : String(4);    // Società
}

entity T001W {
    key WERKS : String(4);   // Codice Divisione
    NAME1     : String(30);  // Nome Divisione
    BWKEY     : String(4);   // Area valorizzazione
}

entity MARC {
    key MATNR : String(18);   // Materiale
    key WERKS : String(4);    // Div. (Divisione / Plant)
    LVORM     : String(1)  @title : 'Indice di cancellazione';
    MMSTA     : String(2) @title : 'Stato materiale';    // SM (Stato mat. spec. divisione);
    MATNR_DIV : Association to one MARA on MATNR_DIV.MATNR = MATNR
}

entity MARA {
    key MATNR    : String(18) @title : 'Materiale';         // Codice Materiale
    ERSDA        : Date       @title : 'Data di creazione'; //@Common.Label: 'Data di creazione' ;          // Creato (Data di creazione)
    ERNAM        : String(12) @title : 'Utente creatore';
    LAEDA        : Date       @title : 'Data ultima modifica';
    AENAM        : String(12) @title : 'Utente ultima modifica';
    LVORM        : String(1)  @title : 'Indice di cancellazione'; 
    MTART        : String(4)  @title : 'Tipo Materiale'; // @Common.FilterDefaultValue: 'ZRIC';     // T.m. (Tipo materiale)
    MATKL        : String(9)  @title : 'Gruppo merci';
    BISMT        : String(18) @title : 'Materiale precedente';
    MEINS        : String(3)  @title : 'Unità di misura';     // UMO base
    BSTME        : String(3)  @title : 'UM ordine d acquisto';     // 
    VOLUM        : Decimal(13,3) @title : 'Volume'; // 
    MSTAE        : String(2)  @title : 'Stato materiale';     // SM 
    ZPART_NUM    : String(30) @title : 'P/N';    // Part Number Fornitore
    LIFNR        : String(10) @title : 'Fornitore';    // Numero conto del fornitore
    MAKTX        : String(40) @title : 'Testo breve materiale';    // 
    MAKTG        : String(40) @title : 'Descrizione';    // Testo breve materiale in lettere maiuscole per matchcodes
    DUPLICATES : Association to many duplicated_material on DUPLICATES.MATNR = MATNR;
    HISTORIC_PN : Association to many ZPN_STOR on HISTORIC_PN.MATNR = MATNR;
    DUP_PARENT   : Association to one duplicated_material on DUP_PARENT.MATNRD = MATNR;
    TESTO_BASE    : Association to one TESTIMAT
        on TESTO_BASE.MATNR = MATNR
        and TESTO_BASE.ZTESTO = 'GRUN';
    TESTO_ACQUISTI : Association to one TESTIMAT
        on TESTO_ACQUISTI.MATNR = MATNR
        and TESTO_ACQUISTI.ZTESTO = 'BEST';
    CE_PARTNUMB  : Association to one AUSP 
        on  CE_PARTNUMB.MATNR = MATNR
        and CE_PARTNUMB.ATINN = '0000000007' @title : 'Part Number';
    CE_EXPARTNUMB : Association to one AUSP
        on CE_EXPARTNUMB.MATNR = MATNR
        and CE_EXPARTNUMB.ATINN = '0000000004';
    CE_CODCSTR    : Association to one AUSP
        on CE_CODCSTR.MATNR = MATNR
        and CE_CODCSTR.ATINN = '0000000001';
    LIFNR_TEXT : Association to one LFA1 on LIFNR_TEXT.LIFNR = LIFNR and LIFNR_TEXT.LAND1 = 'IT';
    CONTRATTI: Association to many DOC_JOIN2 on  CONTRATTI.MATNR = MATNR;
    GIACENZE: Association to many MCHB on GIACENZE.MATNR = MATNR;
    DIVISIONI: Association to many MARC on DIVISIONI.MATNR = MATNR
}

entity AUSP {
    key MATNR   : String(18) @title : 'Materiale';
    key OBJEK   : String(50);      // Codice oggetto da classificare
    key ATINN   : String(10);      // Caratteristica interna
    key ATZHL   : String(3);       // Contatore valore proprietà
    key MAFID   : String(1);       // Codice oggetto/classe
    key KLART   : String(3);       // Tipo classe
    key ADZHL   : String(4);       // Contatore interno per archivio ogg. tramite servizio modif.
    ATWRT       : String(30);      // Valore caratteristica
    ATFLV       : Decimal(16,16);  // Valore interno virgola mobile da
    ATAWE       : String(3);       // UM da
    ATFLB       : Decimal(16,16);  // Valore interno virgola mobile fino
    ATAW1       : String(3);       // UM fino
    ATCOD       : String(1);       // Codice relazione valori
    ATTLV       : Decimal(16,16);  // Tolleranza da
    ATTLB       : Decimal(16,16);  // Tolleranza fino
    ATPRZ       : String(1);       // Codice per specifica di tolleranza in percentuale
    ATINC       : Decimal(16,16);  // Incremento all'interno di una specifica intervallo
    ATAUT       : String(1);       // Classificazione: autore
    AENNR       : String(12);      // Numero modifica
    DATUV       : Date;            // Data inizio validità 
    LKENZ       : String(1);       // Codice cancellazione
    ATIMB       : String(10);      // Numero caratteristiche del tipo di dati definito da utente
    ATZIS       : String(3);       // Contatore istanze
    ATSRT       : String(4);       // Campo sort valutazioni
    ATVGLART    : String(1);       // Tipo confronto valore caratteristica
}

entity ZPN_STOR {
    key MATNR : String(18);   // Materiale
    key CONT  : String(3);    // Contatore
    PARTNUMB  : String(30) @title : 'P/N';   // Part Number Fornitore
    AEDAT     : Date       @title : 'Data';         // Data dell'ultima modifica  
    UZEIT     : Time;         // Ora
    AENAM     : String(12);   // Autore mod. (Utente ultima modifica)

    MATNR_STOR   : Association to one MARA on MATNR_STOR.MATNR = MATNR;
}

entity TESTIMAT {
    key MATNR : String(18);      // Materiale
    key ZTESTO : String(4);      // Tipo testo (GRUN=testo base, BEST=testo di acquisto)
    ZESTESO     : String(1000);  // Testo esteso
}

entity duplicated_material {
    key MATNR  : String(18) @title : 'Materiale';   // Codice materiale principale
    key MATNRD : String(18) @title : 'Materiale duplicato';   // Codice materiale duplicato
    MATCH_SCORE : Integer   @title : '% Corrispondenza';      // Percentuale di corrispondenza (100, 90, 80) % Corrispondenza
    CRITERIO    : String(200) @title : 'Criterio di identificazione';   // Criterio di identificazione
    MATCH_VALUE : String(500) @title : 'match esplicito';   // valore matchato 
    INSERT_DATE : Date @title : 'Data analisi';
    
    MATERIAL     : Association to one MARA on MATERIAL.MATNR = MATNR;
    DUP_MATERIAL : Association to one MARA on DUP_MATERIAL.MATNR = MATNRD;

    TESTO_BASE    : Association to one TESTIMAT
        on TESTO_BASE.MATNR = MATNRD
        and TESTO_BASE.ZTESTO = 'GRUN';

    TESTO_ACQUISTI : Association to one TESTIMAT
        on TESTO_ACQUISTI.MATNR = MATNRD
        and TESTO_ACQUISTI.ZTESTO = 'BEST';
 
    CE_PARTNUMB  : Association to one AUSP 
        on  CE_PARTNUMB.MATNR = MATNRD
        and CE_PARTNUMB.ATINN = '0000000007' ;

    CE_EXPARTNUMB : Association to one AUSP
        on CE_EXPARTNUMB.MATNR = MATNRD
        and CE_EXPARTNUMB.ATINN = '0000000004';

    CE_CODCSTR    : Association to one AUSP
        on CE_CODCSTR.MATNR = MATNRD
        and CE_CODCSTR.ATINN = '0000000001';
  }

entity LFA1 {
    key LIFNR    : String(10) @title : 'Fornitore'; // Numero conto del fornitore
    key LAND1    : String(2)  @title : 'Lingua';  
    NAME1        : String(40) @title : 'Descrizione';   
}

entity EKKO  {
    key EBELN : String(10) @title : 'Numero del documento acquisti';
    BUKRS     : String(4)  @title : 'Codice Società'; 
    BSTYP     : String(1)  @title : 'Categoria del documento acquisti'; 
    BSART     : String(4)  @title : 'Tipo doc. acquisti';
    AEDAT     : Date       @title : 'Data di inserimento del record'; 
    ERNAM     : String(12) @title : 'Utente creatore';
    LIFNR     : String(10) @title : 'Fornitore'; 
    KDATB     : Date       @title : 'Data inizio validità contratto';
    KDATE     : Date       @title : 'Data fine validità contratto';
    ZZATT     : String(1)  @title : 'Flag opzione attiva'; 
    ZZFINE    : Date       @title : 'Data fine validità opzione';  
    FRGKE     : String(1)  @title : 'Indicatore di rilascio documento acquisti'; 
    WAERS     : String(5)  @title : 'Divisa';
}

entity EKPO {
    key EBELN : String(10) @title :'Fonti acquisto';
    key EBELP : String(5)  @title :'Posizione';

    LOEKZ : String(1);
    STATU : String(1);
    AEDAT : Date;
    TXZ01 : String(40);
    MATNR : String(18);
    BUKRS : String(4);
    WERKS : String(4);
    LGORT : String(4);
    MENGE : Decimal(13,3);
    MEINS : String(3);
    NETPR : Decimal(11,2);
    PEINH : Integer;
    NETWR : Decimal(13,2);
    KONNR : String(10)
}

entity MCHB {
    key MATNR : String(18) @title : 'Materiale';
    key WERKS : String(4)  @title : 'Divisione';
    key LGORT : String(4)  @title : 'Magazzino';
    key CHARG : String(10) @title : 'Numero partita';
    CLABS     : Decimal(13,3)  @title : 'Giacenze';
    MATNR_MCHB   : Association to one MARA on MATNR_MCHB.MATNR = MATNR
}

entity tp_company {
    key BUKRS : String(4) @title : 'Codice Società';  // Codice Società
}

entity tp_materialtype {
    key MTART     : String(4) @title : 'Tipo Materiale' ;    // T.m. (Tipo materiale)
}


entity tp_caratteristiche {
    key ATINN   : String(10) @title : 'Codice Caratteristica';   // Caratteristica interna
    PRIORITY    : String(1)  @title : 'Piorità del check sulla caratteristica'   // Piorità del check sulla caratteristica
}

/////Contratti legati ai MATNR     
entity DOC_JOIN0 as
(
    select from db.EKPO
        inner join db.EKKO as Contratti
            on Contratti.EBELN = EKPO.EBELN
    {
        key EKPO.EBELN as EBELN : String(10),
        key MATNR
    }
    where Contratti.BSTYP = 'K'
      and (
            ( current_date between Contratti.KDATB and Contratti.KDATE )
         or ( Contratti.ZZATT = 'X'
              and current_date between Contratti.KDATB and Contratti.ZZFINE )
          )
    group by MATNR, EKPO.EBELN
)

union

(
    select from db.EKPO
        inner join db.EKKO as TestataContratto
            on EKPO.KONNR = TestataContratto.EBELN
    {
        key EKPO.KONNR as EBELN : String(10),
        key MATNR
    }
    where EKPO.KONNR is not null
      and TestataContratto.BSTYP = 'K'
      and (
            ( current_date between TestataContratto.KDATB and TestataContratto.KDATE )
         or ( TestataContratto.ZZATT = 'X'
              and current_date between TestataContratto.KDATB and TestataContratto.ZZFINE )
          )
    group by MATNR, EKPO.KONNR
);

entity DOC_JOIN2 as select from db.DOC_JOIN0
{
    key EBELN : String(10) @title :'Fonti acquisto',
    key MATNR,
    MATNR_EKPO : Association to one MARA on MATNR_EKPO.MATNR = MATNR,
    EBELN_EKKO : Association to one EKKO on EBELN_EKKO.EBELN = EBELN

}group by MATNR, EBELN;


// entity EKBE {
//     key EBELN : String(10);
//     key EBELP : String(5);
//     key VGABE : String(1);
//     key GJAHR : String(4);
//     key BELNR : String(10); 
//     key BUZEI : String(4);
//     BEWTP : String(1);
//     BWART : String(3);
//     BUDAT : Date;
//     MENGE : Decimal(13,3);
//     BPMNG : Decimal(13,3);
// }