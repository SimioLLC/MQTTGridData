using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Xsl;

namespace Simio.Xml
{
    public static class XsltTransform
    {
        public class TransformResult
        {
            public DataSet DataSet { get; set; }
            public string XmlTransformError { get; set; }
            public string DataSetLoadError { get; set; }
        }
        public static TransformResult TransformXmlToDataSet(string rawXml, string stylesheet, Action<string> setFinalXmlResult)
        {
            var memStream = new System.IO.MemoryStream();

            // Attempt to load the XSLT and transform the XML
            try
            {
                var transform = new XslCompiledTransform();
                using (var xsltStrReader = new System.IO.StringReader(stylesheet))
                using (var xsltReader = XmlReader.Create(xsltStrReader))
                {
                    transform.Load(xsltReader);
                }

                using (var xmlStrReader = new System.IO.StringReader(rawXml))
                using (var xmlReader = XmlReader.Create(xmlStrReader))
                using (var xmlWriter = XmlWriter.Create(memStream, new XmlWriterSettings() { ConformanceLevel = ConformanceLevel.Auto }))
                {
                    transform.Transform(xmlReader, xmlWriter);

                    if (setFinalXmlResult != null)
                    {
                        var finalXMLString = xmlWriter.Settings.Encoding.GetString(memStream.GetBuffer(), 0, (int)memStream.Length);
                        setFinalXmlResult(finalXMLString);
                    }
                }
            }
            catch (Exception e)
            {
                var errorString = $"Error processing XSLT: {e.Message}";
                while (e.InnerException != null)
                {
                    errorString += System.Environment.NewLine + e.InnerException.Message;
                    e = e.InnerException;
                }

                return new TransformResult()
                {
                    XmlTransformError = errorString
                };
            }

            // If the XSLT loaded and transformed the XML correctly, now try loading that into a dataset
            try
            {
                var dataSet = new DataSet();
                using (var finalXmlReader = new System.IO.StreamReader(new System.IO.MemoryStream(memStream.GetBuffer(), 0, (int)memStream.Length)))
                    dataSet.ReadXml(finalXmlReader);

                return new TransformResult()
                {
                    DataSet = dataSet
                };
            }
            catch (Exception e)
            {
                var errorString = $"Error processing XML for DataSet Table: {e.Message}";
                while (e.InnerException != null)
                {
                    errorString += System.Environment.NewLine + e.InnerException.Message;
                    e = e.InnerException;
                }

                return new TransformResult()
                {
                    DataSetLoadError = errorString
                };
            }
        }
    }
}
