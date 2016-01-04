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

define([], function() {
    return sap.bi.framework.declareBundle({
        "id" : "com.sap.bi.da.extension.jsonextension",
        "version" : "REPLACE_VERSION",
         "components" : [{
            "id" : "com.sap.bi.da.extension.jsonextension",
            "provide" : "sap.bi.da.extension.client",
            "module" : "JSONExtension",
            "customProperties" : {
                "displayName" : "SAP Lumira JSON Extension",
                "description" : "for SAP Lumira Data Acquisition Framework"
            }
        }],
        "dependencies": ["sap.bi.da.extension.sdk.clientRequestService"]
    });
});
