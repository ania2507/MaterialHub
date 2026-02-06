
using ServiceCatalog as service from '../../srv/service';

// ------------------------------
// MARA - campi e UI di base
// ------------------------------
annotate service.MARA with {
  MATNR       @UI.Hidden : true;
  Criticality @UI.Hidden : true;
  VOLUM       @UI.Hidden : true;
  CountMara   @UI.Hidden : true;
};

annotate service.MARA with @(
  UI.HeaderInfo : {
    TypeName       : 'Materiale',
    TypeNamePlural : 'Materiali',
    Title          : { Value: ZMATNR },
    // Description    : { Value: AnalysisDate }
  },

  UI.FieldGroup #GeneratedGroup : {
    $Type : 'UI.FieldGroupType',
    Data  : [
      { $Type : 'UI.DataField', Value : MAKTG },
      { $Type : 'UI.DataField', Value : MTART },
      { $Type : 'UI.DataField', Value : MEINS },
      { $Type : 'UI.DataField', Value : ERSDA },
      { $Type : 'UI.DataField', Label : 'Giacenza', Value : SommaGiacenze },
      { $Type : 'UI.DataField', Value : MSTAE },
      { $Type : 'UI.DataField', Value : NumFontiAcq },
      { $Type : 'UI.DataField', Value : ZPART_NUM },
      { $Type : 'UI.DataField', Value : LIFNR },
      { $Type : 'UI.DataField', Label : 'Ragione sociale', Value : LIFNR_TEXT.NAME1 }
    ]
  },

  UI.FieldGroup #TestiEstesiGroup : {
    $Type : 'UI.FieldGroupType',
    Data  : [
      { $Type : 'UI.DataField', Label : 'Testo esteso base',     Value : TESTO_BASE.ZESTESO },
      { $Type : 'UI.DataField', Label : 'Testo esteso acquisti', Value : TESTO_ACQUISTI.ZESTESO }
    ]
  },

  UI.FieldGroup #CaratteristicheGroup : {
    $Type : 'UI.FieldGroupType',
    Data  : [
      { $Type : 'UI.DataField', Label : 'P/N fornitore',       Value : CE_PARTNUMB.ATWRT },
      { $Type : 'UI.DataField', Label : 'Codice Costruttore',  Value : CE_EXPARTNUMB.ATWRT },
      { $Type : 'UI.DataField', Label : 'Vecchio part number', Value : CE_CODCSTR.ATWRT }
    ]
  },

  UI.FieldGroup #AcquistatoUltimiMesiGroup : {
    $Type : 'UI.FieldGroupType',
    Data  : [
      { $Type : 'UI.DataField', Value : QtyTotUM },
      { $Type : 'UI.DataField', Value : AvgNetPrice },
      { $Type : 'UI.DataField', Value : TotalValue },
      { $Type : 'UI.DataField', Value : LastPurchaseDate }
    ]
  },
  UI.LineItem : [
 
    { $Type : 'UI.DataField', Value : ZMATNR },
    { $Type : 'UI.DataField', Value : MTART },
    { $Type : 'UI.DataField', Value : ZPART_NUM },
    { $Type : 'UI.DataField', Label : 'Potenziali duplicati', Value : PotenzialiDuplicati },

    { $Type : 'UI.DataField', Label : '% Match (massima)', Value : MaxMatchScore,
      Criticality : Criticality,
      CriticalityRepresentation : #WithIcon },

    { $Type : 'UI.DataField', Label : 'Duplicato padre', Value : DUP_PARENT.ZMATNR }
  ],

  UI.Facets : [
    {
      $Type  : 'UI.CollectionFacet',
      ID     : 'GeneratedFacet1',
      Label  : 'Dati tecnici',
      Facets : [
        { $Type : 'UI.ReferenceFacet', ID : 'InfoGenerali',       Target : '@UI.FieldGroup#GeneratedGroup' },
        { $Type : 'UI.ReferenceFacet', ID : 'TestiEstesi',        Label  : 'Testi Estesi', Target : '@UI.FieldGroup#TestiEstesiGroup' },
        { $Type : 'UI.ReferenceFacet', ID : 'Caratteristiche',    Label  : 'Caratteristiche', Target : '@UI.FieldGroup#CaratteristicheGroup' },
        { $Type : 'UI.ReferenceFacet', ID : 'AcquistatoUltimiMesi', Label : 'Acquistato Ultimi 3 Mesi', Target : '@UI.FieldGroup#AcquistatoUltimiMesiGroup' }
      ]
    },
    { $Type : 'UI.ReferenceFacet', ID : 'DuplicatesFacet',    Label : 'Analisi duplicati', Target : 'DUPLICATES/@UI.LineItem' },
    { $Type : 'UI.ReferenceFacet', ID : 'Historic_pnFacet',   Label : 'Storico P/N',       Target : 'HISTORIC_PN/@UI.LineItem'},
    { $Type : 'UI.ReferenceFacet', ID : 'FontiAcquistoFacet', Label : 'Fonti acquisto',    Target : 'CONTRATTI/@UI.LineItem' },
    { $Type : 'UI.ReferenceFacet', ID : 'GiacenzeFacet',      Label : 'Giacenze',          Target : 'GIACENZE/@UI.LineItem' },
    { $Type : 'UI.ReferenceFacet', ID : 'DivisioniFacet',      Label : 'Divisioni',          Target : 'DIVISIONI/@UI.LineItem' }
  ],

  UI.SelectionFields : [ MTART, ZPART_NUM, ZMATNR, MAKTG, LIFNR ]
);

annotate service.ZPN_STOR with @Capabilities.CountRestrictions: { Countable: true };
annotate service.DOC_JOIN2  with @Capabilities.CountRestrictions: { Countable: true };
annotate service.MCHB       with @Capabilities.CountRestrictions: { Countable: true };
annotate service.duplicated_material with @Capabilities.CountRestrictions: { Countable: true };

// Value Help su MTART e ZMATNR
annotate service.MARA with {
  @Common.ValueList: {
    CollectionPath: 'MaterialsMTART',
    Parameters: [
      { $Type: 'Common.ValueListParameterInOut', LocalDataProperty: 'MTART', ValueListProperty: 'MTART' }
    ]
  } MTART;

  @Common.ValueList: {
    CollectionPath: 'MARA',
    Parameters: [
      { $Type: 'Common.ValueListParameterInOut', LocalDataProperty: 'ZMATNR', ValueListProperty: 'ZMATNR' },
      { $Type: 'Common.ValueListParameterIn',    LocalDataProperty: 'MTART',  ValueListProperty: 'MTART'  },
      { $Type: 'Common.ValueListParameterIn',    LocalDataProperty: 'MAKTG',  ValueListProperty: 'MAKTG'  },
      { $Type: 'Common.ValueListParameterIn',    LocalDataProperty: 'ZPART_NUM', ValueListProperty: 'ZPART_NUM' },
      { $Type: 'Common.ValueListParameterIn',    LocalDataProperty: 'LIFNR',  ValueListProperty: 'LIFNR'  }
    ]
  } ZMATNR;
};
annotate service.MARA with @Capabilities.DeleteRestrictions: {
  Deletable: false
};
// PresentationVariant per la lista MARA
annotate service.MARA with @UI.PresentationVariant: {
  $Type : 'UI.PresentationVariantType',
  SortOrder: [
    { Property : MaxMatchScore, Descending : true }
  ],
  Visualizations: [
    '@UI.LineItem'   ,                // Tabella
    '@UI.Chart#MainKPIMateriali',     // Grafico

  ]
};

annotate service.MARA with @UI.DataPoint #DP_AnalysisDate: {
  Value : AnalysisDate,
  Title : 'Analisi al'
};


// annotate service.MARA with @UI.Chart #KPI_CountMara: {
//   $Type      : 'UI.ChartDefinitionType',
//   Title      : 'Totale Materiali',
//   ChartType  : #Donut,

//   Measures   : [ CountMara ],
//   Dimensions : [ MTART ],

//   MeasureAttributes : [
//     {
//       Measure   : CountMara,
//       Role      : #Axis1,
//       DataPoint : '@UI.DataPoint#DP_CountMara'
//     }
//   ]
// };


// ------------------------------
// Entità di supporto (liste varie)
// ------------------------------
annotate service.duplicated_material with @(
  UI.LineItem : [
    { $Type : 'UI.DataField', Value : ZMATNRD },
    { $Type : 'UI.DataField', Value : MATCH_SCORE },
    { $Type : 'UI.DataField', Value : CRITERIO },
    { $Type : 'UI.DataField', Value : INSERT_DATE },
    { $Type : 'UI.DataField', Label : 'Descrizione',          Value : DUP_MATERIAL.MAKTG },
    { $Type : 'UI.DataField', Label : 'PN',                    Value : DUP_MATERIAL.ZPART_NUM },
    { $Type : 'UI.DataField', Label : 'Fornitore',             Value : DUP_MATERIAL.LIFNR },
    { $Type : 'UI.DataField', Label : 'Ragione sociale',       Value : DUP_MATERIAL.LIFNR_TEXT.NAME1 },
    { $Type : 'UI.DataField', Label : 'Uom',                   Value : DUP_MATERIAL.MEINS },
    { $Type : 'UI.DataField', Label : 'Data creazione',        Value : DUP_MATERIAL.ERSDA },
    { $Type : 'UI.DataField', Label : 'Tipo materiale',        Value : DUP_MATERIAL.MTART },
    { $Type : 'UI.DataField', Label : 'Testo base',            Value : TESTO_BASE.ZESTESO },
    { $Type : 'UI.DataField', Label : 'Testo acquisti',        Value : TESTO_ACQUISTI.ZESTESO },
    { $Type : 'UI.DataField', Label : 'Car. P/N fornitore',    Value : CE_PARTNUMB.ATWRT },
    { $Type : 'UI.DataField', Label : 'Car. Codice Costruttore', Value : CE_EXPARTNUMB.ATWRT },
    { $Type : 'UI.DataField', Label : 'Car. Vecchio part number', Value : CE_CODCSTR.ATWRT },
    { $Type : 'UI.DataField', Label : 'Stato materiale',        Value : DUP_MATERIAL.MSTAE },
    { $Type : 'UI.DataField', Value : MATNR, @UI.Hidden:true },
    { $Type : 'UI.DataField', Value : MATCH_VALUE, @UI.Hidden:true },
    { $Type : 'UI.DataField', Value : ZMATNR, @UI.Hidden:true },
  ]
);

annotate service.duplicated_material with {
  MATNRD   @UI.Hidden : true;
};

annotate service.ZPN_STOR with @(
  UI.LineItem : [
    { $Type : 'UI.DataField', Value : PARTNUMB },
    { $Type : 'UI.DataField', Value : AEDAT }
  ]
);

annotate service.ZPN_STOR with {
  MATNR    @UI.Hidden : true;
  AENAM    @UI.Hidden : true;
  CONT     @UI.Hidden : true;
  UZEIT    @UI.Hidden : true;
};

annotate service.DOC_JOIN2 with @(
  UI.LineItem : [
    { $Type : 'UI.DataField', Value : EBELN },
    { $Type : 'UI.DataField', Value : EBELN_EKKO.KDATB },
    { $Type : 'UI.DataField', Value : EBELN_EKKO.KDATE },
    { $Type : 'UI.DataField', Value : EBELN_EKKO.ZZFINE },
  ]
);

annotate service.DOC_JOIN2 with {
  MATNR       @UI.Hidden : true;
};

annotate service.MCHB with @(
  UI.LineItem : [
    { $Type : 'UI.DataField', Value : GiacenzaConUoM },
    { $Type : 'UI.DataField', Value : LGORT },
    { $Type : 'UI.DataField', Value : WERKS },
    { $Type : 'UI.DataField', Value : CHARG }
  ]
);

annotate service.MCHB with {
  MATNR       @UI.Hidden : true;
  CLABS       @UI.Hidden : true;
};

annotate service.MARC with @(
  UI.LineItem : [
    { $Type : 'UI.DataField', Value : WERKS },
    { $Type : 'UI.DataField', Value : LVORM }, 
    { $Type : 'UI.DataField', Value : MMSTA }
  ]
);
annotate service.MARC with {
  MATNR       @UI.Hidden : true;
};

annotate service.duplicated_material with @Capabilities.SearchRestrictions: {
  Searchable: false
};
annotate service.ZPN_STOR with @Capabilities.SearchRestrictions: {
  Searchable: false
};
annotate service.MARC with @Capabilities.SearchRestrictions: {
  Searchable: false
};
annotate service.MCHB with @Capabilities.SearchRestrictions: {
  Searchable: false
};
annotate service.DOC_JOIN2 with @Capabilities.SearchRestrictions: {
  Searchable: false
};

// ------------------------------
// MARA - KPI/Chart per distribuzione
// ------------------------------

annotate service.MARA with @(
  Aggregation.CustomAggregate #CountMara : 'Edm.Int32',
  Aggregation.CustomAggregate #PotenzialiDuplicati : 'Edm.Int32',
  Aggregation.ApplySupported: {
    Transformations: [ 'aggregate', 'filter', 'groupby', 'search' ],
    GroupableProperties: [ MTART ],
    AggregatableProperties: [
      { Property: CountMara },
      { Property: PotenzialiDuplicati }
    ]
  }
) {
  CountMara @Aggregation.default: #SUM;
  PotenzialiDuplicati @Aggregation.default: #SUM;
};

annotate service.MARA with @UI.DataPoint #DP_CountMara: {
  $Type  : 'UI.DataPointType',
  Value  : CountMara,
  Title  : 'Materiali analizzati',
  Description : 'Numero materiali',
  ValueFormat : {
    $Type : 'UI.NumberFormat',
    NumberOfFractionalDigits : 0
  }
};

annotate service.MARA with @UI.DataPoint #DP_PotenzialiDuplicati: {
  $Type  : 'UI.DataPointType',
  Value  : PotenzialiDuplicati,
  Title  : 'Potenziali duplicati',
  ValueFormat : {
    $Type : 'UI.NumberFormat',
    NumberOfFractionalDigits : 0
  }
};

annotate service.MARA with @UI.KPI #MainKPIMateriali: {
  DataPoint: {
    Value : CountMara,
    Title : 'Materiali analizzati'
  },
  Detail: {
    DefaultPresentationVariant: {
      Visualizations: [ '@UI.LineItem' ]
    }
  }
};


// annotate service.MARA with @UI.KPI #MainKPIMateriali: {
//   DataPoint: {
//     Value: CountMara,
//     Title: 'Materiali analizzati',
//     Description: 'Numero totale materiali'
//   },
//   Detail: {
//     DefaultPresentationVariant: {
//       Visualizations: [
//         '@UI.Chart#MainKPIMateriali'
//       ]
//     }
//   }
// };


annotate service.MARA with @UI.KPI #KPI_PotenzialiDuplicati: {
  DataPoint: {
    Value: PotenzialiDuplicati,
    Title: 'Potenziali duplicati',
    Description: 'Totale duplicati rilevati'
  },
  Detail: {
    DefaultPresentationVariant: {
      Visualizations: [
        '@UI.Chart#MainKPIMateriali'
      ]
    }
  }
};



annotate service.MARA with @UI.Chart #MainKPIMateriali: {
  $Type : 'UI.ChartDefinitionType',
  Title : 'Distribuzione Materiali e Potenziali Duplicati',
  ChartType : #Column,
  Dimensions : [ MTART ],
  Measures   : [ CountMara, PotenzialiDuplicati ],

  DimensionAttributes : [
    { Dimension: MTART, Role: #Category }
  ],

  MeasureAttributes : [
    {
      Measure: CountMara,
      Role: #Axis1,
      DataPoint: '@UI.DataPoint#DP_CountMara'
    },
    {
      Measure: PotenzialiDuplicati,
      Role: #Axis1,
      DataPoint: '@UI.DataPoint#DP_PotenzialiDuplicati'
    }
  ]
};


// // ------------------------------
// // MaterialsWithDuplicates (KPI + Chart)
// // ------------------------------
// annotate service.MaterialsWithDuplicates with @(
//   Aggregation.CustomAggregate #DuplicatedMaterials : 'Edm.Int32',
//   Aggregation.ApplySupported: {
//     Transformations: [ 'aggregate', 'groupby', 'filter', 'search' ],
//     GroupableProperties:   [ MTART ],
//     AggregatableProperties: [ { Property: DuplicatedMaterials } ]
//   }
// ) { DuplicatedMaterials @Aggregation.default: #SUM; };

// annotate service.MaterialsWithDuplicates with @(
//   UI.DataPoint #DP_DuplicatedMaterials: {
//     $Type: 'UI.DataPointType',
//     Value: DuplicatedMaterials,
//   },

//   UI.KPI #MainKPIMaterialiDuplicati: {
//     DataPoint: {
//       Value: DuplicatedMaterials,
//       Title: 'Materiali con potenziali duplicati identificati',
//       Description: 'Numero materiali con almeno un duplicato',
//       CriticalityCalculation: {
//         ImprovementDirection: #Maximize,
//         ToleranceRangeLowValue: 10,
//         DeviationRangeLowValue: 5
//       }
//     },
//     Detail: {
//       DefaultPresentationVariant: {
//         Visualizations: [ '@UI.Chart#ChartDuplicati' ]
//       }
//     }
//   },

//   UI.Chart #ChartDuplicati: {
//     $Type : 'UI.ChartDefinitionType',
//     Title : 'Materiali con duplicati per tipo materiale',
//     ChartType : #Column,
//     Dimensions : [ MTART ],
//     Measures   : [ DuplicatedMaterials ],
//     DimensionAttributes : [ { Dimension: MTART, Role: #Category } ],
//     MeasureAttributes   : [
//       { Measure: DuplicatedMaterials, Role: #Axis1, DataPoint: '@UI.DataPoint#DP_DuplicatedMaterials' }
//     ]
//   }
// );

// annotate service.MaterialStatsByType with @(
//   Aggregation.ApplySupported: {
//     Transformations: [ 'aggregate', 'groupby', 'filter', 'search' ],
//     GroupableProperties: [ MTART ],
//     AggregatableProperties: [
//       { Property: CountMara },
//       { Property: PotenzialiDuplicati }
//     ]
//   }
// );

// annotate service.MaterialStatsByType with @UI.Chart #DualMetrics: {
//   $Type : 'UI.ChartDefinitionType',
//   Title : 'Materiali vs Potenziali Duplicati per Tipo',
//   ChartType : #Column,
//   Dimensions : [ MTART ],
//   Measures   : [ CountMara, PotenzialiDuplicati ],

//   DimensionAttributes : [
//     { Dimension: MTART, Role: #Category }
//   ],

//   MeasureAttributes : [
//     { Measure: CountMara, Role: #Axis1 },
//     { Measure: PotenzialiDuplicati, Role: #Axis1 }
//   ]
// };


annotate service.MARA_Duplicates with {
  PotenzialiDuplicati @odata.Type : 'Edm.Int32';
  MaxMatchScore       @odata.Type : 'Edm.Int32';
};

// ------------------------------
// Dettaglio duplicati (lista)
// ------------------------------
annotate service.duplicated_material_dett with @(
  UI.LineItem : [
    { $Type : 'UI.DataField', Value : MATNR },
    { $Type : 'UI.DataField', Value : MATNRD },
    { $Type : 'UI.DataField', Label : 'PN',                     Value : MATERIAL.ZPART_NUM },
    { $Type : 'UI.DataField', Label : 'Fornitore',              Value : MATERIAL.LIFNR },
    { $Type : 'UI.DataField', Label : 'Data creazione',         Value : MATERIAL.ERSDA },
    { $Type : 'UI.DataField', Label : 'Tipo materiale',         Value : MATERIAL.MTART },
    { $Type : 'UI.DataField', Label : 'PN Duplicato',           Value : DUP_MATERIAL.ZPART_NUM },
    { $Type : 'UI.DataField', Label : 'Fornitore Duplicato',    Value : DUP_MATERIAL.LIFNR },
    { $Type : 'UI.DataField', Label : 'Data creazione Duplicato', Value : DUP_MATERIAL.ERSDA },
    { $Type : 'UI.DataField', Label : 'Tipo materiale Duplicato', Value : DUP_MATERIAL.MTART },
    { $Type : 'UI.DataField', Value : MATCH_SCORE },
    { $Type : 'UI.DataField', Value : CRITERIO },
    { $Type : 'UI.DataField', Label : 'Descrizione',            Value : MATERIAL.MAKTG },
    { $Type : 'UI.DataField', Label : 'Descrizione Duplicato',  Value : DUP_MATERIAL.MAKTG },
    { $Type : 'UI.DataField', Label : 'Car. P/N fornitore',     Value : MATERIAL.CE_PARTNUMB.ATWRT },
    { $Type : 'UI.DataField', Label : 'Car. P/N fornitore Duplicato', Value : DUP_MATERIAL.CE_PARTNUMB.ATWRT },
    { $Type : 'UI.DataField', Label : 'Car. Codice Costruttore',         Value : MATERIAL.CE_EXPARTNUMB.ATWRT },
    { $Type : 'UI.DataField', Label : 'Car. Codice Costruttore Duplicato', Value : DUP_MATERIAL.CE_EXPARTNUMB.ATWRT },
    { $Type : 'UI.DataField', Label : 'Car. Vecchio part number',         Value : MATERIAL.CE_CODCSTR.ATWRT },
    { $Type : 'UI.DataField', Label : 'Car. Vecchio part number Duplicato', Value : DUP_MATERIAL.CE_CODCSTR.ATWRT },
    { $Type : 'UI.DataField', Label : 'Testo base',              Value : MATERIAL.TESTO_BASE.ZESTESO },
    { $Type : 'UI.DataField', Label : 'Testo base Duplicato',    Value : DUP_MATERIAL.TESTO_BASE.ZESTESO },
    { $Type : 'UI.DataField', Label : 'Testo acquisti',          Value : MATERIAL.TESTO_ACQUISTI.ZESTESO },
    { $Type : 'UI.DataField', Label : 'Testo acquisti Duplicato',Value : DUP_MATERIAL.TESTO_ACQUISTI.ZESTESO },
    { $Type : 'UI.DataField', Label : 'Stato Materiale',          Value : MATERIAL.MSTAE },
    { $Type : 'UI.DataField', Label : 'Stato Materiale Duplicato',Value : DUP_MATERIAL.MSTAE },
    { $Type : 'UI.DataField', Value : MATCH_VALUE },
    { $Type : 'UI.DataField', Value : INSERT_DATE }
  ]
);