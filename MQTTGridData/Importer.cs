using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Drawing;
using System.Text;
using System.Threading.Tasks;
using SimioAPI;
using SimioAPI.Extensions;
using System.Security.Permissions;
using System.Net;
using System.Xml.Xsl;
using System.IO;
using System.Xml.XPath;
using System.Xml;
using System.Windows.Forms;
using System.Runtime.Remoting.Contexts;
using System.Threading;

namespace MQTTGridData
{
    public class ImporterDefinition : IGridDataImporterDefinition
    {
        public string Name => "MQTT Importer";
        public string Description => "An importer for requests over MQTT that return XML or JSON results";
        public Image Icon => null;

        static readonly Guid MY_ID = new Guid("885fac37-307f-4f7a-ac2c-0a8667b833fc");
        public Guid UniqueID => MY_ID;

        public IGridDataImporter CreateInstance(IGridDataImporterContext context)
        {
            return new Importer(context);
        }

        public void DefineSchema(IGridDataSchema schema)
        {
            var brokerProp = schema.OverallProperties.AddStringProperty("Broker");
            brokerProp.DisplayName = "Broker";
            brokerProp.Description = "Broker Name or IP Address.";
            brokerProp.DefaultValue = String.Empty;

            var portProp = schema.OverallProperties.AddRealProperty("Port");
            portProp.DisplayName = "Port";
            portProp.Description = "Port.";
            portProp.DefaultValue = 1883;

            var topicsProp = schema.PerTableProperties.AddNameValuePairsProperty("Topics", null);
            topicsProp.DisplayName = "Topics";
            topicsProp.Description = "Subscribe Topic.";
            topicsProp.DefaultValue = String.Empty;

            var qualityOfServiceProp = schema.PerTableProperties.AddListProperty("QualityOfService", MQTTGridDataUtils.QUALITY_OF_SERVICE);
            qualityOfServiceProp.DisplayName = "Quality Of Service";
            qualityOfServiceProp.Description = "Quality Of Service.";
            qualityOfServiceProp.DefaultValue = MQTTGridDataUtils.QUALITY_OF_SERVICE[0].ToString();

            var waitSecondsForRetainedMessagesProp = schema.PerTableProperties.AddRealProperty("WaitSecondsForRetainedMessages");
            waitSecondsForRetainedMessagesProp.DisplayName = "Wait Seconds For Retained Messages";
            waitSecondsForRetainedMessagesProp.Description = "Wait Seconds For Retained Messages.";
            waitSecondsForRetainedMessagesProp.DefaultValue = 1.0;

            var addTopicToMessageProp = schema.PerTableProperties.AddBooleanProperty("AddTopicToMesssage");
            addTopicToMessageProp.DisplayName = "Add Topic To Message";
            addTopicToMessageProp.Description = "Add Topic To Message.";
            addTopicToMessageProp.DefaultValue = true;

            var stylesheetProp = schema.PerTableProperties.AddXSLTProperty("Stylesheet");
            stylesheetProp.Description = "The transform to apply to the data returned from the web request.";
            stylesheetProp.DefaultValue =
@"<xsl:stylesheet version=""1.0"" xmlns:xsl=""http://www.w3.org/1999/XSL/Transform"">
    <xsl:template match=""node()|@*"">
      <xsl:copy>
        <xsl:apply-templates select=""node()|@*""/>
      </xsl:copy>
    </xsl:template>
</xsl:stylesheet>";
            stylesheetProp.GetXML += StylesheetProp_GetXML;

            var requestDebugFileFolderProp = schema.PerTableProperties.AddFilesLocationProperty("RequestDebugFileFolder");
            requestDebugFileFolderProp.DisplayName = "Request Debug File Folder";
            requestDebugFileFolderProp.Description = "Request Debug File Folder.";
            requestDebugFileFolderProp.DefaultValue = String.Empty;

            var responseDebugFileFolderProp = schema.PerTableProperties.AddFilesLocationProperty("ResponseDebugFileFolder");
            responseDebugFileFolderProp.DisplayName = "Response Debug File Folder";
            responseDebugFileFolderProp.Description = "Response Debug File Folder.";
            responseDebugFileFolderProp.DefaultValue = String.Empty;
        }

        private void StylesheetProp_GetXML(object sender, XSLTAddInPropertyGetXMLEventArgs e)
        {
            // This is called when the stylesheet editor pops up. We want to provide the XML data we would expect to come back during an actual import,
            //  so we call that here.
            List<string> debugFiles = new List<string>();
            if (Importer.GetMQTTData(e.HierarchicalProperties[0], e.OtherProperties, null, null, out _, ref debugFiles, out var requestResult, out var error) == false)
            {
                var broker = (string)e.HierarchicalProperties[0]?["Broker"]?.Value;
                var port = Convert.ToInt32((double)e.HierarchicalProperties[0]?["Port"]?.Value);
                var topicsStr = (string)e.OtherProperties?["Topics"]?.Value;
                var topics = AddInPropertyValueHelper.NameValuePairsFromString(topicsStr);
                var topicsArr = topics.Select(z => z.Value).ToArray();
                topicsStr = String.Join(",", topicsArr);
                // If there is an error, let the user know by setting the returned string (that should be displayed to them) to the error
                string message = "Broker = " + broker + ":" + port.ToString() + " on Topics = " + topicsStr + Environment.NewLine + "Error = " + error;

                if (requestResult.Count > 0) requestResult[0] = message;
                else requestResult.Add(message);
            }
            e.XML = requestResult[0];
        }
    }

    class Importer : IGridDataImporter
    {
        public Importer(IGridDataImporterContext context)
        {
        }
                
        /// <summary>
        /// Call to get the web data, either via http on via the sessionCache
        /// </summary>
        /// <returns>True if successful, False if there is an error</returns>
        internal static bool GetMQTTData(INamedSimioCollection<IAddInPropertyValue> overallSettings, INamedSimioCollection<IAddInPropertyValue> tableSettings,
            INamedSimioCollection<IGridDataSettings> gridDataSettings, string tableName, out string stylesheet, ref List<string> requestDebugFiles, 
            out List<string> requestResults, out string error)
        {
            requestResults = new List<string>();
            error = null;
            stylesheet = null;
            string requestDebugFileFolder = null;
            string responseDebugFileFolder = null;
            //
            // Harvest raw values
            var broker = (string)overallSettings?["Broker"]?.Value;
            var port = Convert.ToInt32((double)overallSettings?["Port"]?.Value);
            var topicsStr = (string)tableSettings?["Topics"]?.Value;
            var topics = AddInPropertyValueHelper.NameValuePairsFromString(topicsStr);
            var topicsArr = topics.Select(z => z.Value).ToArray();
            topicsStr = String.Join(",", topicsArr);
            var qualityOfService = (string)tableSettings?["QualityOfService"]?.Value;
            var waitSecondsForRetainedMessages = (double)tableSettings?["WaitSecondsForRetainedMessages"]?.Value;
            var addTopicToMessage = (bool)tableSettings?["AddTopicToMesssage"]?.Value;
            stylesheet = (string)tableSettings?["Stylesheet"]?.Value;
            requestDebugFileFolder = (string)tableSettings?["RequestDebugFileFolder"]?.Value;
            responseDebugFileFolder = (string)tableSettings?["ResponseDebugFileFolder"]?.Value;

            if (String.IsNullOrWhiteSpace(broker))
            {
                error = "Broker overall parameter is not specified";
                return false;
            }

            if (topics.Count() == 0)
            {
                error = "Topics table parameter is not specified";
                return false;
            }

            if (String.IsNullOrWhiteSpace(stylesheet))
            {
                error = "Stylesheet table parameter is not specified";
                return false;
            }

            try
            {
                // if debug mode
                if (requestDebugFileFolder != null && requestDebugFileFolder.Length > 0)
                {
                    if (requestDebugFiles.Count == 0)
                    {
                        string[] files = Directory.GetFiles(requestDebugFileFolder);
                        foreach (string file in files)
                        {
                            requestDebugFiles.Add(file);
                        }
                    }
                    else
                    {
                        requestDebugFiles.RemoveAt(0);
                    }
                    if (requestDebugFiles.Count > 0)
                    {
                        foreach (string file in requestDebugFiles)
                        {
                            string result = File.ReadAllText(file);
                            if (MQTTGridDataUtils.checkIsProbablyJSONObject(result))
                            {
                                XmlDocument xmlDoc = Newtonsoft.Json.JsonConvert.DeserializeXmlNode(result, "data");
                                requestResults.Add(xmlDoc.OuterXml);
                            }
                        }
                    }
                }
                else
                {
                    MQTTGridDataUtils.Responses.Clear();

                    var subscribeError = MQTTGridDataUtils.SubscribeTopicsAsync(tableName, broker, port, topics, qualityOfService).Result;
                    Thread.Sleep((int)Math.Floor(waitSecondsForRetainedMessages * 1000));
                    var unsubscribeError = MQTTGridDataUtils.UnSubscribeTopicsAsync(tableName, broker, port, topics).Result;
                    var disconnectError = MQTTGridDataUtils.DisconnectAsync();
                    if (subscribeError.Length > 0)
                    {
                        throw new Exception(subscribeError);
                    }
                    if (subscribeError.Length > 0)
                    {
                        throw new Exception(subscribeError);
                    }
                    if (unsubscribeError.Length > 0)
                    {
                        throw new Exception(unsubscribeError);
                    }
                    if (disconnectError.Length > 0)
                    {
                        throw new Exception(disconnectError);
                    }
                    foreach (var msg in MQTTGridDataUtils.Responses)
                    {
                        requestResults.Add(MQTTGridDataUtils.ParseDataToXML(msg[0], addTopicToMessage, msg[1], responseDebugFileFolder, out var parseError));
                        if (parseError.Length > 0)
                        {
                            throw new Exception(parseError);
                        }
                    }                    
                    System.Diagnostics.Trace.TraceInformation("Success Retrieving Data from Broker: " + broker + " on Topics " + topicsStr);
                }
            }
            catch (Exception e)
            {
                requestResults.Clear();
                error = $"There was an error attempting to connect to Broker: '{broker}' on Topics: {topicsStr}.  Response: {e.Message}";

#warning Remove this stack trace addition, we probably don't want to leak this information
                error += "\nStack trace:" + e.StackTrace;

                // This is ok, it should just write to the local machine... at least we *think* that's ok...
                System.Diagnostics.Trace.TraceError(error + "\nStack trace:" + e.StackTrace);
                return false;
            }           

            return true;
        }

        public OpenImportDataResult OpenData(IGridDataOpenImportDataContext openContext)
        {
            Int32 numberOfRows = 0;
            var mergedDataSet = new DataSet();
            List<string> requestDebugFiles = new List<string>();

            if (GetMQTTData(openContext.Settings.Properties, openContext.Settings.GridDataSettings[openContext.TableName].Properties,
                openContext.Settings.GridDataSettings, openContext.TableName, out var stylesheet, ref requestDebugFiles, out var requestResults, out var error) == false)
            {
                return OpenImportDataResult.Failed(error);
            }
            //
            // Generate the DataSet. Note that we are NOT caching the resulting DataSet here. Unlike the web request, we believe the combination of URL + stylesheet will 
            //  probably generally be unique per table. Plus, unlike the web request, we don't need to worry about temporal changes causes changes in the underlying data.
            //
            if (requestResults.Count > 0)
            { 
                foreach (var requestResult in requestResults)
                {
                    var transformedResult = Simio.Xml.XsltTransform.TransformXmlToDataSet(requestResult, stylesheet, null);
                    if (transformedResult.XmlTransformError != null)
                        return new OpenImportDataResult() { Result = GridDataOperationResult.Failed, Message = transformedResult.XmlTransformError };
                    if (transformedResult.DataSetLoadError != null)
                        return new OpenImportDataResult() { Result = GridDataOperationResult.Failed, Message = transformedResult.DataSetLoadError };
                    if (transformedResult.DataSet.Tables.Count > 0) numberOfRows = transformedResult.DataSet.Tables[0].Rows.Count;
                    else numberOfRows = 0;
                    if (numberOfRows > 0)
                    {
                        transformedResult.DataSet.AcceptChanges();
                        if (mergedDataSet.Tables.Count == 0) mergedDataSet.Merge(transformedResult.DataSet);
                        else mergedDataSet.Tables[0].Merge(transformedResult.DataSet.Tables[0]);
                        mergedDataSet.AcceptChanges();
                    }
                    var xmlDoc = new XmlDocument();
                    xmlDoc.LoadXml(requestResult);
                }
            }
            else
            {
                numberOfRows = 0;
            }

            // If no rows found by importer, create result data table with zero rows, but the same set of columns from the table so importer does not error out saying "no column names in data source match existing column names in table"
            if (mergedDataSet.Tables.Count == 0)
            {
                var zeroRowTable = new DataTable();
                var columnSettings = openContext.Settings.GridDataSettings[openContext.TableName]?.ColumnSettings;
                if (columnSettings != null)
                {
                    foreach (var cs in columnSettings)
                    {
                        zeroRowTable.Columns.Add(cs.ColumnName);
                    }
                }
                mergedDataSet.Tables.Add(zeroRowTable);
            }

            //
            // Return the result
            //
            return new OpenImportDataResult()
            {
                Result = GridDataOperationResult.Succeeded,
                Records = new MQTTGridDataRecords(mergedDataSet)
            };
        }

        public string GetDataSummary(IGridDataSummaryContext context)
        {
            if (context == null)
                return null;

            var broker = (string)context.Settings.Properties["Broker"]?.Value;
            var topicsStr = (string)context.Settings.GridDataSettings[context.GridDataName]?.Properties["Topics"]?.Value;
            var topics = AddInPropertyValueHelper.NameValuePairsFromString(topicsStr);
            var topicsArr = topics.Select(z => z.Value).ToArray();
            topicsStr = String.Join(",", topicsArr);

            return String.Format("Bound to broker {0} on topics {1}", broker, topicsStr);
        }

        public void Dispose()
        {

        }
    }

    class MQTTGridDataRecords : IGridDataRecords
    {
        readonly DataSet _dataSet;

        public MQTTGridDataRecords(DataSet dataSet)
        {
            _dataSet = dataSet;
        }

        #region IGridDataRecords Members

        List<GridDataColumnInfo> _columnInfo;
        List<GridDataColumnInfo> ColumnInfo
        {
            get
            {
                if (_columnInfo == null)
                {
                    _columnInfo = new List<GridDataColumnInfo>();

                    if (_dataSet.Tables.Count > 0)
                    {
                        foreach (DataColumn dc in _dataSet.Tables[0].Columns)
                        {
                            var name = dc.ColumnName;
                            var type = dc.DataType;

                            _columnInfo.Add(new GridDataColumnInfo()
                            {
                                Name = name,
                                Type = type
                            });
                        }
                    }
                }

                return _columnInfo;
            }
        }

        public IEnumerable<GridDataColumnInfo> Columns
        {
            get { return ColumnInfo; }
        }

        #endregion

        #region IEnumerable<IGridDataRecord> Members

        public IEnumerator<IGridDataRecord> GetEnumerator()
        {
            if (_dataSet.Tables.Count > 0)
            {
                foreach (DataRow dr in _dataSet.Tables[0].Rows)
                {
                    yield return new MQTTGridDataRecord(dr);
                }

            }
        }

        #endregion

        #region IEnumerable Members

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        { 

        }

        #endregion
    }

    class MQTTGridDataRecord : IGridDataRecord
    {
        private readonly DataRow _dr;
        public MQTTGridDataRecord(DataRow dr)
        {
            _dr = dr;
        }

        #region IGridDataRecord Members

        public string this[int index]
        {
            get
            {
                var theValue = _dr[index];

                // Simio will first try to parse dates in the current culture
                if (theValue is DateTime)
                    return ((DateTime)theValue).ToString();

                return String.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}", _dr[index]);
            }
        }

        #endregion
    }
}