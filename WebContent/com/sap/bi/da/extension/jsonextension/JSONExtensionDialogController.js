/*
Copyright 2015, SAP SE

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    
       http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

define(function() {
    "use strict";

    var JSONExtensionDialogController = function(acquisitionState, oDeferred, fServiceCall, workflow, ExtensionUtils) {

    	/*
        Create dialog controls
        */
        var dLayout = new sap.ui.commons.layout.MatrixLayout({
            layoutFixed : true,
            columns : 2,
            width : "570px",
            widths : [ "20%", "80%" ]
        });
        
        // Dataset Name
        var datasetNameTxt = new sap.ui.commons.TextField({
            width : '100%',
            value : "",
            enabled : workflow === "CREATE"
        });

        var datasetNameLbl = new sap.ui.commons.Label({
            text : "Dataset Name:",
            labelFor : datasetNameTxt
        });

        dLayout.createRow({
            height : "30px"
        }, datasetNameLbl, datasetNameTxt);
        
        var JSONFilePathTxt = new sap.ui.commons.TextField({
            width : '100%',
            value : "C:\\"
        });

        var JSONFilePathLbl = new sap.ui.commons.Label({
            text : "JSON File:",
            labelFor : JSONFilePathTxt
        });
      
        dLayout.createRow({
            height : "30px"
        }, JSONFilePathLbl, JSONFilePathTxt);
        
        var browseFile = function(JSONFilePathTxt, oEvent) {
            var ext = window.viMessages.getText("JSON") + "\0*.json";
            var filePath = app.fileOpenDialog(ext);
            if (filePath){
            	JSONFilePathTxt.setValue(filePath);
            	var filename = filePath.replace(/^.*[\\\/]/, '');
            	filename = filename.substr(0, filename.lastIndexOf('.'));
            	datasetNameTxt.setValue(filename);
                //this.inputChanged(oEvent);
            }
        };
        
        var importButton = new sap.ui.commons.Button({
            press : browseFile.bind(this, JSONFilePathTxt),
            text : "Browse File",
            tooltip : "Browse File"
        }).addStyleClass(sap.ui.commons.ButtonStyle.Emph);

        var BrowseButtonLbl = new sap.ui.commons.Label({
            text : "",
            labelFor : importButton
        });
        
        dLayout.createRow({
            height: "30px"
        }, BrowseButtonLbl, importButton);
        
        /*
        Button press events
        */
        var buttonCancelPressed = function() {
        	oDeferred.reject(); //promise fail
            dialog.close(); // dialog is hoisted from below
        };
        
        var buttonOKPressed = function() {
            var info = {};
            
            info.datasetName =  datasetNameTxt.getValue();
            info.jsonfilepath = JSONFilePathTxt.getValue();
            
            acquisitionState.info = JSON.stringify(info);
            oDeferred.resolve(acquisitionState, datasetNameTxt.getValue());
            dialog.close();
        };

        var okButton = new sap.ui.commons.Button({
            press : [ buttonOKPressed, this ],
            text : "Import",
            tooltip : "Import"
        }).setStyle(sap.ui.commons.ButtonStyle.Accept);

        var cancelButton = new sap.ui.commons.Button({
            press : [ buttonCancelPressed, this ],
            text : "Cancel",
            tooltip : "Cancel"
        }).addStyleClass(sap.ui.commons.ButtonStyle.Default);

        var onClosed = function() {
            if (oDeferred.state() === "pending") {
                oDeferred.reject();
            }
        };
        
        /*
        Modify controls based on acquisitionState
        */
        var envProperties = acquisitionState.envProps;
        if (acquisitionState.info) {
            var info = JSON.parse(acquisitionState.info);

            JSONFilePathTxt.setValue(info.jsonfilepath);
            
            envProperties.datasetName = info.datasetName;
        }
        datasetNameTxt.setValue(envProperties.datasetName);
        
        /*
        Create the dialog
        */
        var dialog = new sap.ui.commons.Dialog({
            width : "720px",
            height : "480px",
            modal : true,
            resizable : false,
            closed : function () {
                this.destroy();
                oDeferred.reject();
            },
            content: [dLayout],
            buttons : [okButton, cancelButton]
        });
        
        dialog.setTitle("JSON: " + envProperties.datasetName);

        this.showDialog = function() {
            dialog.open();
        };
    };

    return JSONExtensionDialogController;
});