using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using SimioAPI;
using SimioAPI.Extensions;
using System.Net;
using System.IO;
using System.Xml;
using System.Xml.Linq;
using System.Xml.Xsl;
using System.Xml.XPath;
using System.Runtime.InteropServices;
using System.Threading;
using System.Web.UI.WebControls;
using System.Runtime.Remoting.Messaging;
using MQTTnet.Client;
using MQTTnet.Extensions.ManagedClient;
using MQTTnet;
using MQTTnet.Server;

namespace MQTTGridData
{

    public class MQTTGridDataUtils
    {
        public static string[] QUALITY_OF_SERVICE = new string[] { "ATLEASTONCE", "EXACTLYONCE", "ATMOSTONCE" };
        public static string[] STATUS_SEVERITY = new string[] { "ERRORSANDWARNINGS", "ALL" };
        public static string[] EXPORT_TYPE = new string[] { "COLUMNMAPPING", "JSONOBJECT", "JSONARRAY" };
        public static MqttFactory MQTTFactory = null;
        public static IManagedMqttClient MQTTClient = null;
        public static List<string> Responses = new List<string>();

        /// <summary>
        /// Sends a web request, and gets back XML data. If the raw data returned from the request is JSON it is converted to XML.
        /// </summary>
        /// <returns>The XML data returned from the web request</returns>
        ///         
        internal static async Task<String> SubscribeTopicsAsync(string clientId, string broker, Int32 port, IDictionary<string, string> topics, string qos)
        {
            var responseError = String.Empty;

            if (MQTTClient == null)
            {
                MQTTFactory = new MqttFactory();
                MQTTClient = MQTTFactory.CreateManagedMqttClient();

                // Create client options object
                MqttClientOptionsBuilder builder = new MqttClientOptionsBuilder()
                                                        .WithClientId(clientId)
                                                        .WithTcpServer(broker, port);
                                                
                ManagedMqttClientOptions options = new ManagedMqttClientOptionsBuilder()
                                        .WithAutoReconnectDelay(TimeSpan.FromSeconds(60))
                                        .WithClientOptions(builder.Build())
                                        .Build();                

                await MQTTClient.StartAsync(options);

            }

            try
            {
                MQTTClient.ApplicationMessageReceivedAsync += MQTTClient_ApplicationMessageReceivedAsync;
                var topicsArr = topics.Select(z => z.Value).ToArray();
                foreach (var topic in topicsArr)
                {
                    if (qos == QUALITY_OF_SERVICE[0])
                        await MQTTClient.SubscribeAsync(topic, MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce);
                    else if (qos == QUALITY_OF_SERVICE[1])
                        await MQTTClient.SubscribeAsync(topic, MQTTnet.Protocol.MqttQualityOfServiceLevel.ExactlyOnce);
                    else
                        await MQTTClient.SubscribeAsync(topic, MQTTnet.Protocol.MqttQualityOfServiceLevel.AtMostOnce); 
                }
            }            
            catch (Exception ex)
            {
                responseError = ex.Message;
            }
            return responseError;
        }

        internal static Task MQTTClient_ApplicationMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs arg)
        {
            Responses.Add(Encoding.UTF8.GetString(arg.ApplicationMessage.Payload, 0, arg.ApplicationMessage.Payload.Length));
            return Task.CompletedTask;
        }

        internal static async Task<String> UnSubscribeTopicsAsync(string clientId, string broker, Int32 port, IDictionary<string, string> topics)
        {
            var responseError = String.Empty;

            if (MQTTClient == null)
            {
                MQTTFactory = new MqttFactory();
                MQTTClient = MQTTFactory.CreateManagedMqttClient();

                // Create client options object
                MqttClientOptionsBuilder builder = new MqttClientOptionsBuilder()
                                                        .WithClientId(clientId)
                                                        .WithTcpServer(broker, port);
                ManagedMqttClientOptions options = new ManagedMqttClientOptionsBuilder()
                                        .WithAutoReconnectDelay(TimeSpan.FromSeconds(60))
                                        .WithClientOptions(builder.Build())
                                        .Build();

                

                await MQTTClient.StartAsync(options);

            }
            try
            {
                MQTTClient.ApplicationMessageReceivedAsync -= MQTTClient_ApplicationMessageReceivedAsync;
                var topicsArr = topics.Select(z => z.Value).ToArray();
                foreach (var topic in topicsArr)
                {
                    await MQTTClient.UnsubscribeAsync(topic);
                }               
            }
            catch (Exception ex)
            {
                responseError = ex.Message;
            }
            return responseError;
        }

        internal static async Task<String> PublishMessageAsync(string clientId, string broker, Int32 port, string topic, string message, string qos, bool retainMessage)
        {
            var responseError = String.Empty;

            try
            {
                if (MQTTClient == null)
                { 
                    MQTTFactory = new MqttFactory();
                    MQTTClient = MQTTFactory.CreateManagedMqttClient();

                    // Create client options object
                    MqttClientOptionsBuilder builder = new MqttClientOptionsBuilder()
                                                            .WithClientId(clientId)
                                                            .WithTcpServer(broker, port);
                    ManagedMqttClientOptions options = new ManagedMqttClientOptionsBuilder()
                                            .WithAutoReconnectDelay(TimeSpan.FromSeconds(60))
                                            .WithClientOptions(builder.Build())
                                            .Build();

                    await MQTTClient.StartAsync(options);

                }

                byte[] bytes = Encoding.ASCII.GetBytes(message);

                if (qos == QUALITY_OF_SERVICE[0])
                    await MQTTClient.EnqueueAsync(topic, message, MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce, retainMessage);
                else if (qos == QUALITY_OF_SERVICE[1])
                    await MQTTClient.EnqueueAsync(topic, message, MQTTnet.Protocol.MqttQualityOfServiceLevel.ExactlyOnce, retainMessage);
                else
                    await MQTTClient.EnqueueAsync(topic, message, MQTTnet.Protocol.MqttQualityOfServiceLevel.AtMostOnce, retainMessage);
            }
            catch (Exception ex)
            {
                responseError = ex.Message;
            }
            return responseError;   
        }

        internal static string DisconnectAsync()
        {
            var responseError = String.Empty;

            if (MQTTClient != null)
            {
                try
                {
                    MQTTClient.Dispose();
                    MQTTClient = null;
                }
                catch (Exception ex)
                {
                    responseError = ex.Message;
                }
            }
            return responseError;
        }

        internal static string ParseDataToXML(string responseString, string responseDebugFileFolder, out string responseError)
        {
            responseError = String.Empty;

            if (responseDebugFileFolder.Length > 0) saveDebugResponseString(responseDebugFileFolder, responseString);

            // no response
            if (responseString.Length == 0) return responseString;

            bool isXMLResponse = false;
            bool isProbablyJSONObject = false;
            XmlDocument xmlDoc;
            if (responseString.Contains("xml"))
            {
                isXMLResponse = true;
            }
            else
            {
                isProbablyJSONObject = checkIsProbablyJSONObject(responseString);
            }

            if (isXMLResponse)
            {
                return responseString;
            }
            else // Default to assume a JSON response
            {
                xmlDoc = JSONToXMLDoc(responseString, isProbablyJSONObject);
            }

            return xmlDoc.InnerXml;
        }

        internal static void logStatus(string dataConnector, string pathAndFilename, string sendText, string responseError, string deliminator, double exportStartTimeOffsetHours)
        {
            try
            {
                using (System.IO.StreamWriter file = new System.IO.StreamWriter(pathAndFilename, true))
                {
                    string statusText = System.DateTime.Now.AddHours(exportStartTimeOffsetHours).ToString() + deliminator + dataConnector + deliminator + sendText + deliminator;
                    if (responseError.Length == 0) statusText += "Success" + deliminator + responseError;
                    else statusText += "Error" + deliminator + responseError;
                    file.WriteLine(statusText);
                }
            }
            catch { }
        }

        internal static void saveDebugResponseString(string path, string responseString)
        {
            try
            {
                var guidStr = Guid.NewGuid().ToString();
                string pathAndFilename = path;
                pathAndFilename += guidStr;
                pathAndFilename += ".txt";
                using (System.IO.StreamWriter file = new System.IO.StreamWriter(pathAndFilename, false))
                {
                    file.WriteLine(responseString);
                }
            }
            catch { }
        }

        internal static bool checkIsProbablyJSONObject(string resultString)
        {
            // We are looking for the first non-whitespace character (and are specifically not Trim()ing here
            //  to eliminate memory allocations on potentially large (we think?) strings
            foreach (var theChar in resultString)
            {
                if (Char.IsWhiteSpace(theChar))
                    continue;

                if (theChar == '{')
                {
                    return true;
                }
                else if (theChar == '<')
                {
                    return false;
                }
                else
                {
                    break;
                }
            }
            return false;
        }

        internal static XmlDocument JSONToXMLDoc(string resultString, bool isProbablyJSONObject)
        {
            XmlDocument xmlDoc;
            resultString = resultString.Replace("@", string.Empty);
            if (isProbablyJSONObject == false)
            {
                var prefix = "{ items: ";
                var postfix = "}";

                using (var combinedReader = new StringReader(prefix)
                                            .Concat(new StringReader(resultString))
                                            .Concat(new StringReader(postfix)))
                {
                    var settings = new JsonSerializerSettings
                    {
                        Converters = { new Newtonsoft.Json.Converters.XmlNodeConverter() { DeserializeRootElementName = "data" } },
                        DateParseHandling = DateParseHandling.None,
                    };
                    using (var jsonReader = new JsonTextReader(combinedReader) { CloseInput = false, DateParseHandling = DateParseHandling.None })
                    {
                        xmlDoc = JsonSerializer.CreateDefault(settings).Deserialize<XmlDocument>(jsonReader);
                    }
                }
            }
            else
            {
                xmlDoc = Newtonsoft.Json.JsonConvert.DeserializeXmlNode(resultString, "data");
            }
            return xmlDoc;
        }
    }
}