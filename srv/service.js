const cds = require("@sap/cds");
const ExcelJS = require("exceljs");

function removeLeadingZeros(value) {
  if (value === null || value === undefined) return value;
  return String(value).replace(/^0+/, "") || "0";
}

function toExcelDate(value) {
  if (!value) return null;
  return new Date(value); // converte "yyyy-mm-dd" → Date
}


module.exports = cds.service.impl(function () {

  this.on("DownloadDuplicatiXLSX", async (req) => {

    console.log("📥 DownloadDuplicatiXLSX chiamata");

    // =====================================================
    // 1) LETTURA FILTRO DALLA URL
    // =====================================================

    const rawFilter = req.data?.filter;
    console.log("📥 Filtro parametro:", rawFilter);

    let where = [];
    let mtartValues = [];
    const groupedValues = {};   // { MTART: [], LIFNR: [], MAKTG: [], ... }

    if (rawFilter) {
      const f = rawFilter.trim();
      console.log("🧪 Filtro decodificato:", f);

      // intercetta tutte le condizioni del tipo: CAMPO eq 'VALORE'
      const regex = /(\w+)\s+eq\s+'([^']+)'/gi;
      let match;

      const fieldMap = {
        MTART:     { ref: ["MATERIAL", "MTART"] },
        LIFNR:     { ref: ["MATERIAL", "LIFNR"] },
        MAKTG:     { ref: ["MATERIAL", "MAKTG"] },
        ZPART_NUM: { ref: ["MATERIAL", "ZPART_NUM"] }
      };

      while ((match = regex.exec(f)) !== null) {
        const field = match[1].toUpperCase();
        const value = match[2];

        console.log("🎯 Condizione:", field, "=", value);

        if (!groupedValues[field]) {
          groupedValues[field] = [];
        }
        groupedValues[field].push(value);

        // serve solo per costruire il filename
        if (field === "MTART") {
          mtartValues.push(value);
        }
      }

      // Costruzione WHERE:
      //  - stesso campo  => OR
      //  - campi diversi => AND
      Object.entries(groupedValues).forEach(([field, values]) => {
        const ref = fieldMap[field];
        if (!ref) return;

        if (where.length) where.push("and");

        const uniqueValues = [...new Set(values)];
        const fieldWhere = [];

        uniqueValues.forEach((val, idx) => {
          if (idx > 0) fieldWhere.push("or");
          fieldWhere.push(ref, "=", { val });
        });

        if (fieldWhere.length > 1) {
          where.push("(", ...fieldWhere, ")");
        } else {
          where.push(...fieldWhere);
        }
      });
    }



    // =====================================================
    // 2) QUERY CON EXPAND + FILTRI
    // =====================================================
    let query = SELECT.from("ServiceCatalog.duplicated_material_dett")
      .columns(
        "MATNR",
        "MATNRD",
        "MATCH_SCORE",
        "CRITERIO",
        "MATCH_VALUE",
        "INSERT_DATE",
        {
          ref: ["MATERIAL"],
          expand: [
            "*",
            { ref: ["CE_PARTNUMB"], expand: ["*"] },
            { ref: ["CE_EXPARTNUMB"], expand: ["*"] },
            { ref: ["CE_CODCSTR"], expand: ["*"] },
            { ref: ["TESTO_BASE"], expand: ["*"] },
            { ref: ["TESTO_ACQUISTI"], expand: ["*"] }
          ]
        },
        {
          ref: ["DUP_MATERIAL"],
          expand: [
            "*",
            { ref: ["CE_PARTNUMB"], expand: ["*"] },
            { ref: ["CE_EXPARTNUMB"], expand: ["*"] },
            { ref: ["CE_CODCSTR"], expand: ["*"] },
            { ref: ["TESTO_BASE"], expand: ["*"] },
            { ref: ["TESTO_ACQUISTI"], expand: ["*"] }
          ]
        }
      );

    // applicazione reale del filtro
    if (where.length) {
      console.log("✅ Where finale:", JSON.stringify(where, null, 2));
      query.where(where);
    }

    const rows = await cds.run(query);
    console.log(`📊 Righe estratte: ${rows.length}`);

    // =====================================================
    // 3) CREAZIONE EXCEL
    // =====================================================
    const wb = new ExcelJS.Workbook();
    const ws = wb.addWorksheet("Duplicati");

    ws.columns = [
      { header: "Materiale", key: "MATNR", width: 18 },
      { header: "Materiale Duplicato", key: "MATNRD", width: 18 },

      { header: "PN", key: "PN", width: 18 },
      { header: "Fornitore", key: "LIFNR", width: 15 },
      { header: "Data creazione", key: "ERSDA", width: 15 },
      { header: "Tipo materiale", key: "MTART", width: 15 },

      { header: "PN Duplicato", key: "DUP_PN", width: 18 },
      { header: "Fornitore Duplicato", key: "DUP_LIFNR", width: 15 },
      { header: "Data creazione Duplicato", key: "DUP_ERSDA", width: 18 },
      { header: "Tipo materiale Duplicato", key: "DUP_MTART", width: 18 },

      { header: "Match %", key: "MATCH_SCORE", width: 10 },
      { header: "Criterio", key: "CRITERIO", width: 20 },

      { header: "Descrizione", key: "MAKTG", width: 40 },
      { header: "Descrizione Duplicato", key: "DUP_MAKTG", width: 40 },

      { header: "Car. P/N fornitore", key: "CE_PN", width: 25 },
      { header: "Car. P/N forn. Dup", key: "DUP_CE_PN", width: 25 },

      { header: "Car. Codice Costruttore", key: "CE_COSTR", width: 30 },
      { header: "Car. Codice Costruttore Dup", key: "DUP_CE_COSTR", width: 30 },

      { header: "Car. Vecchio part number", key: "CE_OLDPN", width: 30 },
      { header: "Car. Vecchio part number Dup", key: "DUP_CE_OLDPN", width: 30 },

      { header: "Testo base", key: "TESTO_BASE", width: 40 },
      { header: "Testo base Duplicato", key: "DUP_TESTO_BASE", width: 40 },

      { header: "Testo acquisti", key: "TESTO_ACQ", width: 40 },
      { header: "Testo acquisti Duplicato", key: "DUP_TESTO_ACQ", width: 40 },

      { header: "Stato Materiale", key: "MSTAE", width: 18 },
      { header: "Stato Materiale Duplicato", key: "DUP_MSTAE", width: 22 },
      
      { header: "Match esplicito", key: "MATCH_VALUE", width: 15 },
      { header: "Data inserimento", key: "INSERT_DATE", width: 15 }
    ];

    rows.forEach(r => {
      ws.addRow({
        MATNR: removeLeadingZeros(r.MATNR),
        MATNRD: removeLeadingZeros(r.MATNRD),

        PN: r.MATERIAL?.ZPART_NUM,
        LIFNR: r.MATERIAL?.LIFNR,
        ERSDA: toExcelDate(r.MATERIAL?.ERSDA),
        MTART: r.MATERIAL?.MTART,

        DUP_PN: r.DUP_MATERIAL?.ZPART_NUM,
        DUP_LIFNR: r.DUP_MATERIAL?.LIFNR,
        DUP_ERSDA: r.DUP_MATERIAL?.ERSDA,
        DUP_ERSDA: toExcelDate(r.DUP_MATERIAL?.ERSDA),
        DUP_MTART: r.DUP_MATERIAL?.MTART,

        MATCH_SCORE: r.MATCH_SCORE,
        CRITERIO: r.CRITERIO,

        MAKTG: r.MATERIAL?.MAKTG,
        DUP_MAKTG: r.DUP_MATERIAL?.MAKTG,

        CE_PN: r.MATERIAL?.CE_PARTNUMB?.ATWRT,
        DUP_CE_PN: r.DUP_MATERIAL?.CE_PARTNUMB?.ATWRT,

        CE_COSTR: r.MATERIAL?.CE_EXPARTNUMB?.ATWRT,
        DUP_CE_COSTR: r.DUP_MATERIAL?.CE_EXPARTNUMB?.ATWRT,

        CE_OLDPN: r.MATERIAL?.CE_CODCSTR?.ATWRT,
        DUP_CE_OLDPN: r.DUP_MATERIAL?.CE_CODCSTR?.ATWRT,

        TESTO_BASE: r.MATERIAL?.TESTO_BASE?.ZESTESO,
        DUP_TESTO_BASE: r.DUP_MATERIAL?.TESTO_BASE?.ZESTESO,

        TESTO_ACQ: r.MATERIAL?.TESTO_ACQUISTI?.ZESTESO,
        DUP_TESTO_ACQ: r.DUP_MATERIAL?.TESTO_ACQUISTI?.ZESTESO,

        MSTAE: r.MATERIAL?.MSTAE,
        DUP_MSTAE: r.DUP_MATERIAL?.MSTAE,
 
        MATCH_VALUE: r.MATCH_VALUE,
        INSERT_DATE: toExcelDate(r.INSERT_DATE),
      });
    });

    // =====================================================
    // 4) FORMATTAZIONE
    // =====================================================
    const headerRow = ws.getRow(1);

    headerRow.eachCell((cell, colNumber) => {
      const column = ws.getColumn(colNumber);
      const key = column.key || "";

      const isDuplicato =
        key.startsWith("DUP_") || key === "MATNRD";

      cell.font = {
        bold: true,
        color: { argb: "FFFFFFFF" }
      };

      cell.fill = {
        type: "pattern",
        pattern: "solid",
        fgColor: {
          argb: isDuplicato
            ? "FFED7D31" // 🟧 duplicati
            : "FF4472C4" // 🟦 originale
        }
      };

      cell.alignment = {
        vertical: "middle",
        horizontal: "center"
      };

      cell.border = {
        top:    { style: "thin" },
        left:   { style: "thin" },
        bottom: { style: "thin" },
        right:  { style: "thin" }
      };
    });


    // ws.getRow(1).font = { bold: true };
    ws.autoFilter = "A1:Y1";

    ws.getColumn("MATNR").numFmt  = "@";
    ws.getColumn("MATNRD").numFmt = "@";

    ws.getColumn("ERSDA").numFmt = "dd/mm/yyyy";
    ws.getColumn("DUP_ERSDA").numFmt = "dd/mm/yyyy";
    ws.getColumn("INSERT_DATE").numFmt = "dd/mm/yyyy";

    // =====================================================
    // 5) DOWNLOAD BINARIO
    // =====================================================
    const buffer = await wb.xlsx.writeBuffer();
    console.log("✅ Excel buffer:", buffer.length, "bytes");

    const res = req._.res;
    res.status(200);
    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );

    // =====================================================
    // Nome file dinamico
    // =====================================================
    const today = new Date();
    const yyyy = today.getFullYear();
    const mm = String(today.getMonth() + 1).padStart(2, "0");
    const dd = String(today.getDate()).padStart(2, "0");
    const dateStr = `${yyyy}${mm}${dd}`;

    let filename = `Duplicati_${dateStr}`;

    if (mtartValues.length) {
      // rimuove duplicati e caratteri strani
      const uniqueMtart = [...new Set(mtartValues)]
        .map(v => v.replace(/[^a-zA-Z0-9_-]/g, ""))
        .join("-");

      filename += `_Tipo_${uniqueMtart}`;
    }

    filename += `.xlsx`;

    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${filename}"`
    );

    res.setHeader("Content-Length", buffer.length);

    res.end(Buffer.from(buffer));
    return;
  });

});
