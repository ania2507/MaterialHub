using db from '../db/schema';

service ServiceCatalog {
    @requires:'Admin'
    entity T001K as projection on db.T001K;

    entity T001W as projection on db.T001W;

    entity MARC as projection on db.MARC {
        *,
        MATNR_DIV  : Association to one MARA on MATNR_DIV.MATNR = MATNR
    };

    entity AUSP as projection on db.AUSP;

    entity TESTIMAT as projection on db.TESTIMAT;

    entity ZPN_STOR as projection on db.ZPN_STOR {
        *,
        MATNR_STOR  : Association to one MARA on MATNR_STOR.MATNR = MATNR
    };
    
    entity EKKO as projection on db.EKKO;
    entity EKPO as projection on db.EKPO;
    //entity EKPO_L as projection on db.EKPO where coalesce(trim(LOEKZ), '') = '';
    // entity EKBE as projection on db.EKBE;

    entity DOC_JOIN0 as projection on db.DOC_JOIN0;

    entity DOC_JOIN2 as projection on db.DOC_JOIN2{  
        *,
        MATNR_EKPO  : Association to one MARA on MATNR_EKPO.MATNR = MATNR,
        EBELN_EKKO : Association to one EKKO on EBELN_EKKO.EBELN = EBELN
    };

    entity MCHB as projection on db.MCHB {
        *,
        MATNR_MCHB  : Association to one MARA on MATNR_MCHB.MATNR = MATNR,
        concat(CLABS, ' ', MATNR_MCHB.MEINS) as GiacenzaConUoM : String @title : 'Giacenze',
    };

    entity LFA1 as projection on db.LFA1;
    @requires:'Admin'
    entity tp_company as projection on db.tp_company;
    @requires:'Admin'
    entity tp_materialtype as projection on db.tp_materialtype;

    entity tp_caratteristiche as projection on db.tp_caratteristiche;

    
    entity MARA as select from db.MARA  as a 
             left join MARA_Duplicates as b on a.MATNR = b.MATNR 
             left join PurchLastMonths as c on a.MATNR = c.MATNR{
        key a.MATNR,
        ltrim(a.MATNR, '0') as ZMATNR : String(18) @Common : { Text : MAKTX, TextArrangement : #TextLast } @title : 'Materiale',  // Rimuove gli zeri iniziali
        a.ERSDA,       
        a.ERNAM,        
        a.LAEDA,        
        a.AENAM, 
        a.LVORM,       
        a.MTART,        
        a.MATKL,       
        a.BISMT,        
        a.MEINS,       
        a.BSTME,  
        a.VOLUM,     
        a.ZPART_NUM,   
        a.LIFNR,        
        a.MAKTX,
        a.MAKTG,
        a.MSTAE,   
        coalesce(b.PotenzialiDuplicati, 0) as PotenzialiDuplicati : Integer @title : 'Potenziali duplicati',
        b.MaxMatchScore,
        DUPLICATES, // Navigation property
        b.AnalysisDate ,
        HISTORIC_PN,
        DUP_PARENT,
        TESTO_BASE,
        TESTO_ACQUISTI,
        CE_PARTNUMB,
        CE_EXPARTNUMB,
        CE_CODCSTR,
        case
            when b.MaxMatchScore = 100 then 3
            when b.MaxMatchScore = 90 then 2
            when b.MaxMatchScore = 80 then 1
            else 0
        end as Criticality : Integer,
        LIFNR_TEXT,
        CONTRATTI,
        ( select count( distinct x.EBELN )
            from db.DOC_JOIN2 as x
            where x.MATNR = a.MATNR
        ) as NumFontiAcq : Integer @title : 'Fonti acquisto',
        GIACENZE,
        concat(( select sum(y.CLABS)
        from db.MCHB as y
        where y.MATNR = a.MATNR
        ) , ' ', a.MEINS) as SommaGiacenze: String @title : 'Giacenze',
        concat(c.QtyTot, ' ',a.MEINS) as QtyTotUM: String @title :'Quantità totale',
        concat(c.AvgNetPrice, ' ',c.WAERS) as AvgNetPrice: String @title : 'Prezzo unitario medio',
        concat(c.TotalValue, ' ',c.WAERS)  as TotalValue: String  @title : 'Valore totale',
        c.LastPurchaseDate,
        DIVISIONI,
        1 as CountMara : Integer @title : 'Numero Materiali',
       } where a.LVORM != 'X';
    //    actions {           
    //     @cds.odata.bindingparameter.collection
    //     //     @Core.OperationAvailable: true              // forza la disponibilità
    //     //     @Common.OperationGrouping: #Isolated        // esegui fuori da change set
    //     //     action DownloadDuplicatiXLSX() returns DownloadResponse;
    //     action DownloadDuplicatiXLSX()
    //     returns String
    //     @Core.MediaType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' ;
    //     } 

    function DownloadDuplicatiXLSX(filter : String)
     returns Binary
     @Core.MediaType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';


    

  /* === Tipo di ritorno per l'azione di download === */
//   type DownloadResponse : {
//     fileName      : String;
//     mimeType      : String;
//     contentBase64 : LargeString;
//   };


    @cds.redirection.target
    entity duplicated_material as projection on db.duplicated_material
    {
        *,
        ltrim(MATNRD, '0') as ZMATNRD : String(18) @title : 'Materiale Duplicato',  // Rimuove gli zeri iniziali
        ltrim(MATNR, '0') as ZMATNR : String(18) @title : 'Duplicato padre',  // Rimuove gli zeri iniziali
        MATERIAL: Association to one MARA on MATERIAL.MATNR     = MATNR,   // target = ServiceCatalog.MARA
        DUP_MATERIAL : Association to one MARA on DUP_MATERIAL.MATNR = MATNRD   // target = ServiceCatalog.MARA        
    } order by MATCH_SCORE desc;
    
    entity duplicated_material_dett as select from db.duplicated_material{
        *,
        MATERIAL: Association to one MARA on MATERIAL.MATNR     = MATNR,   // target = ServiceCatalog.MARA
        DUP_MATERIAL : Association to one MARA on DUP_MATERIAL.MATNR = MATNRD   // target = ServiceCatalog.MARA        
        } order by MATCH_SCORE desc, MATNR asc;

      //   Vista aggregata per materiali con duplicati    
    entity MARA_Duplicates as select from db.duplicated_material as d  {
            key d.MATNR,
            count(d.MATNRD)    as PotenzialiDuplicati : Integer  @title : 'Potenziali Duplicati',   
            max(d.MATCH_SCORE) as MaxMatchScore       : Integer, 
            MATERIAL.MTART   as MTART,
            min(d.INSERT_DATE) as AnalysisDate     : Date       
        }
    group by d.MATNR, MATERIAL.MTART;

/////OdA legati ai MATNR 
    entity ODA_JOIN1 as select from db.EKPO as EKPO
    inner join db.EKKO as EKKO on EKKO.EBELN = EKPO.EBELN
    inner join db.MARA as MARA on MARA.MATNR = EKPO.MATNR
    {
        key EKPO.EBELN,   
        key EKPO.EBELP,
        EKPO.AEDAT,
        EKPO.WERKS,
        EKPO.MATNR,
        EKPO.LGORT,
        EKKO.LIFNR,
        EKPO.NETPR,
        EKPO.NETWR,
        EKPO.MENGE,
        EKPO.MEINS,
        EKPO.TXZ01,
        EKKO.WAERS
    }
    where EKKO.BSTYP = 'F'
      and EKKO.BSART <> 'UB' //escludiamo i documenti di trasporto 
      and EKPO.AEDAT >= add_months(current_date, -63)
      and EKKO.FRGKE IN ('B',''); // indicatore di rilascio, solo ordini approvati

    entity PurchLastMonths as select from (
    select from ODA_JOIN1 {
        MATNR,
        MENGE,
        NETPR,
        NETWR,
        AEDAT,
        WAERS
        } ) as Y
        {
            key MATNR,
            round(sum(Y.MENGE), 0) as QtyTot : Integer @title : 'Quantità totale',
            // sum(Y.MENGE)           as QtyTot : Decimal(13,3) @title : 'Quantità totale',
            WAERS,
            sum(Y.NETWR)           as TotalValue : Decimal(11,2) @title :'Valore totale',
            sum(Y.NETPR)           as AvgNetPrice : Decimal(11,2) @title :  'Prezzo unitario medio', 
            max(Y.AEDAT)           as LastPurchaseDate : Date @title : 'Data ultimo acquisto'
        }
        where Y.AEDAT >= add_months(current_date, -63)
        group by MATNR, WAERS;


    entity MaterialsMTART as select from db.MARA {
        key MTART
    }
    group by MTART;

//*  /*  /*  /*  /*  /*  /*  /*  /*  /*  /*  /*  /* KPI GRAFICI

    // entity MaterialsWithDuplicates as select from MARA_Duplicates as c {
    //     key c.MTART,
    //     count(c.MATNR) as DuplicatedMaterials : Integer @title : 'Materiali Potenzialmente Duplicati'
    // }
    // group by c.MTART;

    // entity MaterialStatsByType as select from MARA as M
    // left join MARA_Duplicates as D
    // on D.MTART = M.MTART
    // {
    // key M.MTART                            as MTART,
    //     sum(1)                            as CountMara : Integer,
    //     sum( coalesce(D.PotenzialiDuplicati, 0) )
    //                                     as PotenzialiDuplicati : Integer
    // }
    // group by M.MTART;


    entity to_hide as select distinct key MATNRD from db.duplicated_material;
      
    // entity MARA as select from db.MARA as a
    // left join to_hide as b on a.MATNR = b.MATNRD {
    //     key a.MATNR as pippo,
    //     a.*,
    //     case when b.MATNRD is null then '' else 'X' end as to_hide : String(1)
    // };
}
