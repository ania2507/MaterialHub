sap.ui.define([
    "sap/ui/core/mvc/ControllerExtension",
    "sap/m/MessageToast"
], function (ControllerExtension, MessageToast) {
    "use strict";

    return ControllerExtension.extend("materialhub.ext.controller.ActionHandler", {
        metadata: {
            methods: {
                publicMethods: ["onExportDuplicati"]
            }
        },

        override: {},

        /**
         * Custom action handler for "Esporta Duplicati (XLSX)" button.
         * Reads current ALP filter conditions, builds a filter string,
         * then calls the DownloadDuplicatiXLSX OData function import.
         */
        onExportDuplicati: function () {
            var sFilter = "";

            try {
                var oView = null;
                if (this && this.base && typeof this.base.getView === "function") {
                    oView = this.base.getView();
                } else if (this && typeof this.getView === "function") {
                    oView = this.getView();
                }

                var oFilterBar = oView
                    ? oView.byId("materialhub::MARAList--fe::FilterBar::MARA")
                    : sap.ui.getCore().byId("materialhub::MARAList--fe::FilterBar::MARA");
                if (oFilterBar && typeof oFilterBar.getFilterConditions === "function") {
                    var oConditions = oFilterBar.getFilterConditions();
                    var aFilters = [];

                    Object.keys(oConditions).forEach(function (sField) {
                        var aCond = oConditions[sField];
                        if (!Array.isArray(aCond)) { return; }
                        aCond.forEach(function (oCond) {
                            if (oCond.values && oCond.values.length) {
                                var rawValue = String(oCond.values[0]);
                                var escapedValue = rawValue.replace(/'/g, "''");
                                aFilters.push(sField + " eq '" + escapedValue + "'");
                            }
                        });
                    });

                    sFilter = aFilters.join(" and ");
                }
            } catch (e) {
                console.warn("Could not read filter bar conditions:", e);
            }

            var safeFilter = (sFilter || "").replace(/'/g, "''");
            var sUrl =
                "/odata/v4/service-catalog/DownloadDuplicatiXLSX(filter='" +
                encodeURIComponent(safeFilter) +
                "')";

            MessageToast.show("Download in corso…");

            var xhr = new XMLHttpRequest();
            xhr.open("GET", sUrl, true);
            xhr.responseType = "blob";

            xhr.onload = function () {
                if (xhr.status === 200) {
                    var blob = xhr.response;
                    var url = window.URL.createObjectURL(blob);
                    var disposition = xhr.getResponseHeader("Content-Disposition") || "";
                    var fileNameMatch = disposition.match(/filename="?([^";]+)"?/i);
                    var fileName = fileNameMatch && fileNameMatch[1];
                    if (!fileName) {
                        var now = new Date();
                        var yyyy = now.getFullYear();
                        var mm = String(now.getMonth() + 1).padStart(2, "0");
                        var dd = String(now.getDate()).padStart(2, "0");
                        fileName = "Duplicati_" + yyyy + mm + dd + ".xlsx";
                    }
                    var a = document.createElement("a");
                    a.href = url;
                    a.download = fileName;
                    document.body.appendChild(a);
                    a.click();
                    document.body.removeChild(a);
                    window.URL.revokeObjectURL(url);
                    MessageToast.show("Download completato");
                } else {
                    MessageToast.show("Errore durante il download: " + xhr.status);
                }
            };

            xhr.onerror = function () {
                MessageToast.show("Errore di rete durante il download");
            };

            xhr.send();
        }
    });
});
