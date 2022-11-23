using SimioAPI.Extensions;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Data;
using System.Globalization;
using System.Xml.Serialization;
using Newtonsoft.Json;

namespace MQTTGridData
{
    public class ExporterDefinition : IGridDataExporterDefinition
    {
        public string Name => "MQTT Exporter";
        public string Description => "An exporter for row data to be sent as requests over MQTT";
        public Image Icon => null;

        static readonly Guid MY_ID = new Guid("4b55856e-f5f1-409d-9093-f1a8dcf407ba");
        public Guid UniqueID => MY_ID;
        public IGridDataOverallSettings Settings { get; set; }

        public IGridDataExporter CreateInstance(IGridDataExporterContext context)
        {
            return new Exporter(context);
        }

        public void DefineSchema(IGridDataSchema schema)
        {
            var brokerProp = schema.OverallProperties.AddStringProperty("Broker");
            brokerProp.DisplayName = "Broker";
            brokerProp.Description = "Broker Name or IP Address.";
            brokerProp.DefaultValue = String.Empty;

            var messageProp = schema.OverallProperties.AddStringProperty("Message");
            messageProp.DisplayName = "Message";
            messageProp.Description = "The message to post if Method is set to POST. Can contain tokens (in the form ${name}, or of the form ${col:Name} to use column values by colum name) to be later replaced by per-table settings.";
            messageProp.DefaultValue = String.Empty;

            var exportStartTimeStringProp = schema.OverallProperties.AddStringProperty("ExportStartTimeString");
            exportStartTimeStringProp.DisplayName = "Export Start Time String";
            exportStartTimeStringProp.Description = "Used to specify the export start time based on the models regional setting.  If left blank or valid, the time of the computer will be used.";
            exportStartTimeStringProp.DefaultValue = "${datetimeregionnow:s:0}";

            var topicProp = schema.PerTableProperties.AddStringProperty("Topic");
            topicProp.DisplayName = "Topic";
            topicProp.Description = "Subscribe Topic.";
            topicProp.DefaultValue = String.Empty;

            var tableTokenReplacementsProp = schema.PerTableProperties.AddNameValuePairsProperty("TokenReplacements", null);
            tableTokenReplacementsProp.DisplayName = "Token Replacements";
            tableTokenReplacementsProp.Description = "Values used for tokens specified in either the URL or Message of the Data Connector. The values here can themselves contains tokens of the form ${col:Name} to use column values by colum name.";
            tableTokenReplacementsProp.DefaultValue = String.Empty;

            var numberOfRowsPerCallProp = schema.PerTableProperties.AddRealProperty("NumberOfRowsPerCall");
            numberOfRowsPerCallProp.DisplayName = "Number Of Rows Per Call";
            numberOfRowsPerCallProp.Description = "Defines number of rows from the table that should be sent per web API call.   " +
                "If this number is less that the number or rows in the table, the export will call the web API multiple time.   " +
                "By default, this value should be set to 1.    " +
                "If set to one, a Row Message and Row Delimiter are not needed.  " +
                "The column mapping can happen within the Message.   T" +
                "he Row Message can still be use if the value is set to 1, but the Row Delimiter will not be used since a message will be sent after each row is read.";
            numberOfRowsPerCallProp.DefaultValue = 1;

            var exportTypePropTableProperties = schema.PerTableProperties.AddListProperty("ExportType", MQTTGridDataUtils.EXPORT_TYPE);
            exportTypePropTableProperties.DisplayName = "Export Type";
            exportTypePropTableProperties.Description = "Defined the type of export.   If COLUMNMAPPING, the export will map columns defined in the URL, Form Parameters, " +
                "Message and Row Message to current row in the data table.  This type is useful if the required message needed for the destination is different than the structure " +
                "of the source table/log.  COLUMNMAPPING use the ${rowmessage} token replacement to define where in the message the rows should be placed. COLUMNMAPPING does not require the " +
                "${rowmessage} token replacement...The column mapping can be placed in the URL, Form Parameters or Message, but this approarch only works if the rows per call is set to 1. JSONOBJECT " +
                "will convert the source table/log into a JSON Object and send the JSON Object.  JSONObject use the ${jsonobject} token replacement to define where in the message the rows should be placed. " +
                "JSONARRAY will convert the source table/log into a JSON Array.    JSONARRAY use the ${jsonarray} token replacement to define where in the message the rows should be placed."; 
            exportTypePropTableProperties.DefaultValue = MQTTGridDataUtils.EXPORT_TYPE[0].ToString();

            var rowMessageProp = schema.PerTableProperties.AddStringProperty("RowMessage");
            rowMessageProp.DisplayName = "Row Message";
            rowMessageProp.Description = "Message that will be added per row.   The column mappings will typically be defined within the Row Message.  The ${RowMessage} token replacement is used to define " +
                "where in the message the row message(s) should be placed.";
            rowMessageProp.DefaultValue = String.Empty;

            var rowDelimiterProp = schema.PerTableProperties.AddStringProperty("RowDelimiter");
            rowDelimiterProp.DisplayName = "Row Delimiter";
            rowDelimiterProp.Description = "Text that will separate each row.   For a JSON message, the delimiter is typically a comma.   For an XML message, there typically will not be a row delimiter.";
            rowDelimiterProp.DefaultValue = ",";

            var statusFileNameProp = schema.PerTableProperties.AddFileProperty("StatusFileName");
            statusFileNameProp.DisplayName = "Status File Name";
            statusFileNameProp.Description = "Status File Name.";
            statusFileNameProp.DefaultValue = String.Empty;

            var statusDelimiterProp = schema.PerTableProperties.AddStringProperty("StatusDelimiter");
            statusDelimiterProp.DisplayName = "Status Delimiter";
            statusDelimiterProp.Description = "Status Delimiter.";
            statusDelimiterProp.DefaultValue = ";";

            var statusSeverityProp = schema.PerTableProperties.AddListProperty("StatusSeverity", MQTTGridDataUtils.STATUS_SEVERITY);
            statusSeverityProp.DisplayName = "Status Severity";
            statusSeverityProp.Description = "Status contains Errors and Warnings (ERRORSANDWARNINGS) or Everything (ALL).";
            statusSeverityProp.DefaultValue = MQTTGridDataUtils.STATUS_SEVERITY[0];

            var responseDebugFileFolderProp = schema.PerTableProperties.AddFilesLocationProperty("ResponseDebugFileFolder");
            responseDebugFileFolderProp.DisplayName = "Response Debug File Folder";
            responseDebugFileFolderProp.Description = "Response Debug File Folder.";
            responseDebugFileFolderProp.DefaultValue = String.Empty;
        }
    }

    class Exporter : IGridDataExporter
    {
        public Exporter(IGridDataExporterContext context)
        {
        }

        public OpenExportDataResult OpenData(IGridDataOpenExportDataContext openContext)
        {
            var mySettings = openContext.Settings;
            var overallSettings = openContext.Settings.Properties;
            var exportStartTimeString = (string)overallSettings?["ExportStartTimeString"]?.Value;
            var exportStartTime = DateTime.MinValue;
            if (DateTime.TryParse(exportStartTimeString, out exportStartTime)) { }
            else exportStartTime = DateTime.Now;
            var exportStartTimeOffsetHours = (exportStartTime - DateTime.Now).TotalHours;
            var tableSettings = openContext.Settings.GridDataSettings[openContext.GridDataName]?.Properties;
            var numberOfRowsPerCall = Convert.ToInt32(tableSettings?["NumberOfRowsPerCall"]?.Value);
            if (numberOfRowsPerCall < 0) throw new Exception("Number Of Rows Per Call must be greater than zero.");
            int currentRowNumber = 0;
            int currentRowNumberInCall = 0;
            var broker = (string)overallSettings?["Broker"]?.Value;
            var topic = (string)tableSettings?["Stylesheet"]?.Value;
            var stylesheet = (string)tableSettings?["Stylesheet"]?.Value;
            var requestDebugFileFolder = (string)tableSettings?["RequestDebugFileFolder"]?.Value;
            var exportType = (string)tableSettings?["ExportType"]?.Value;
            var rowMessage = (string)tableSettings?["RowMessage"]?.Value;
            string currentRowMessage = String.Empty;
            string currentCallRows = String.Empty;
            var rowDelimiter = (string)tableSettings?["RowDelimiter"]?.Value;
            var statusFileName = (string)tableSettings?["StatusFileName"]?.Value;
            var statusDelimiter = (string)tableSettings?["StatusDelimiter"]?.Value;
            var statusSeverity = (string)tableSettings?["StatusSeverity"]?.Value;
            var responseDebugFileFolder = (string)tableSettings?["ResponseDebugFileFolder"]?.Value;

            // Create the log message list
            List<String> errorLog = new List<String>();

            // setup datatable
            DataTable dataTable = dataTable = new DataTable();
            dataTable.TableName = openContext.GridDataName;
            dataTable.Locale = CultureInfo.InvariantCulture;
            if (exportType.ToUpper() == "JSONOBJECT")
            {
                foreach (var col in openContext.Records.Columns)
                {
                    var dtCol = dataTable.Columns.Add(col.Name, col.Type);
                }
            }

            // setup dataArray
            List<object[]> dataArray = new List<object[]>();

            int numberOfColumns = openContext.Records.Columns.Count();
            int numberOfRows = openContext.Records.Count();
            var finalMessage = string.Empty;

            foreach (var record in openContext.Records)
            {
                currentRowNumber++;
                currentRowNumberInCall++;
                var thisRecordValues = new Dictionary<string, string>();
                if (exportType.ToUpper() == "COLUMNMAPPING")
                {                    
                    int colIndex = 0;
                    foreach (var col in openContext.Records.Columns)
                    {
                        thisRecordValues[col.Name] = GetFormattedStringValue(record, colIndex);
                        colIndex++;
                    }

                    MQTTGridDataUtils.MapFinalValuesFromExportRecordValues(overallSettings, tableSettings, thisRecordValues, ref finalUrl, ref finalMessage, ref finalFormParameters);
                }

                try
                {
                    string webRequestResult = null;
                    
                    if (exportType.ToUpper() == "COLUMNMAPPING")
                    {
                        currentRowMessage = TokenReplacement.ResolveString(rowMessage, null, thisRecordValues, null);

                        if (currentCallRows.Length > 0)
                            currentCallRows += rowDelimiter + currentRowMessage;
                        else
                            currentCallRows = currentRowMessage;
                    }
                    else if (exportType.ToUpper() == "JSONOBJECT")
                    {
                        object[] thisRow = new object[numberOfColumns];
                        int columnIndex = 0;
                        foreach (var col in openContext.Records.Columns)
                        {
                            string formattedValue = GetFormattedStringValue(record, columnIndex);
                            thisRow[columnIndex] = formattedValue;
                            columnIndex++;
                        }

                        dataTable.Rows.Add(thisRow);
                    }
                    else
                    {
                        object[] thisRow = new object[numberOfColumns];
                        int columnIndex = 0;
                        foreach (var col in openContext.Records.Columns)
                        {
                            thisRow[columnIndex] = record.GetNativeObject(columnIndex);
                            columnIndex++;
                        }                               
                                
                        dataArray.Add(thisRow);
                    }

                    if (numberOfRowsPerCall <= currentRowNumberInCall || currentRowNumber >= numberOfRows)
                    {
                        if (exportType.ToUpper() == "JSONOBJECT")
                        {
                            currentCallRows = JsonConvert.SerializeObject(dataTable);
                            dataTable.Clear();
                            var rowCallTokenReplacement = new Dictionary<string, string>()
                            {
                                ["jsonobject"] = currentCallRows
                            };
                            finalMessage = TokenReplacement.ResolveString(finalMessage, rowCallTokenReplacement, null, null);
                        }
                        else if (exportType.ToUpper() == "JSONARRAY")
                        {
                            currentCallRows = JsonConvert.SerializeObject(dataArray);
                            dataArray.Clear();
                            var rowCallTokenReplacement = new Dictionary<string, string>()
                            {
                                ["jsonarray"] = currentCallRows
                            };
                            finalMessage = TokenReplacement.ResolveString(finalMessage, rowCallTokenReplacement, null, null);
                        }
                        else
                        {
                            var rowCallTokenReplacement = new Dictionary<string, string>()
                            {
                                ["rowmessage"] = currentCallRows
                            };
                            finalMessage = TokenReplacement.ResolveString(finalMessage, rowCallTokenReplacement, null, null);
                        }

                        string traceText = "URL:" + broker + " - Topic:" + topic + " - Message:" + finalMessage;
                        System.Diagnostics.Trace.TraceInformation(traceText);
                        MQTTGridDataUtils.SubscribeToGetXmlData(headers, null, authenticationtype, credentials, useDefaultCredentials, networkCredential, msalParameters, finalUrl, method, finalMessage, finalFormParameters, responseDebugFileFolder, out var responseHeaders, out var responseCookies, out var responseError, out var responseWarning);
                        if (statusFileName.Length > 0 && (statusSeverity.ToLower().Contains("all") || responseWarning.Length > 0 || responseError.Length > 0))
                        {
                            MQTTGridDataUtils.logStatus(openContext.GridDataName, statusFileName, traceText, responseError, responseWarning, statusDelimiter, exportStartTimeOffsetHours);
                        }
                        currentRowNumberInCall = 0;
                        currentCallRows = String.Empty;
                        if (responseError.Length > 0) throw new Exception(responseError);
                    }                               
  
                }
                catch (Exception e)
                {
                    var error = $"There was an error attempting to connect to Broker: '{broker}' on Topic: {topic}.  Response: {e.Message}";
                    errorLog.Add(error);
                }
                
            }

            if (errorLog.Count == 0) return OpenExportDataResult.Succeeded();
            else
            {
                string errors = string.Join(System.Environment.NewLine, errorLog.ToArray());

                string traceErrors = "Errors:" + errors;
                System.Diagnostics.Trace.TraceError(traceErrors);

                return OpenExportDataResult.Failed(errors);
            }            
        }

        private static void GetValues(string tableName, IGridDataOverallSettings settings, out string folderName, out string fileName, out bool bUseHeaders, out string separator, out string culture)
        {
            fileName = (string)settings.GridDataSettings[tableName]?.Properties["FileName"]?.Value;
            bUseHeaders = (bool)settings.Properties["WriteHeaders"].Value;
            folderName = (string)settings.Properties["ExportFolder"]?.Value; // ExportFolder was not defined in previous versions, hence the ?. null-conditional check here
            var separatorstr = (string)settings.Properties["Separator"].Value;
            if (String.IsNullOrWhiteSpace(separatorstr))
                separatorstr = ",";
            separator = separatorstr;
            culture = (string)settings.Properties["ExportCulture"].Value;
        }

        public string GetDataSummary(IGridDataSummaryContext context)
        {
            if (context == null)
                return null;

            var url = (string)context.Settings.Properties["URL"]?.Value;
            var tokenReplacementsStr = (string)context.Settings.GridDataSettings[context.GridDataName]?.Properties["TokenReplacements"]?.Value;
            var tokenReplacements = AddInPropertyValueHelper.NameValuePairsFromString(tokenReplacementsStr);

            return $"Bound to URL: {TokenReplacement.ResolveString(url, tokenReplacements, null, null) ?? "[None]"}";
        }

        public void Dispose()
        {
        }

        private static string GetFormattedStringValue(IGridDataExportRecord record, Int32 colIdx)
        {
            var valueStr = record.GetString(colIdx) ?? "null";

            // Special handle certain types, to make sure they follow the export culture
            var valueObj = record.GetNativeObject(colIdx);
            if (valueObj is DateTime)
            {
                valueStr = String.Format(CultureInfo.InvariantCulture, "{0}", valueObj);
            }
            else if (valueObj is double valueDouble)
            {
                if (Double.IsPositiveInfinity(valueDouble))
                    valueStr = "Infinity";
                else if (Double.IsNegativeInfinity(valueDouble))
                    valueStr = "-Infinity";
                else if (Double.IsNaN(valueDouble))
                    valueStr = "NaN";
                else
                    valueStr = String.Format(CultureInfo.InvariantCulture, "{0}", valueDouble);
            }
            return valueStr;
        }       
    }
}
