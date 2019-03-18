using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace ImportData.PetroLogistics
{
    public enum ReportFormat { csv, xml, json };

    public class ReportParams
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sAPI_HTTP_USER"></param>
        /// <param name="sAPI_HTTP_PASS"></param>
        /// <param name="sAPI_KEY"></param>
        /// <param name="sAPI_HASH"></param>
        /// <param name="sDOMAIN"></param>
        /// <param name="sPATH"></param>
        /// <param name="sQUERY"></param>
        /// <param name="eREPORT"></param>
        public ReportParams(String sAPI_HTTP_USER, String sAPI_HTTP_PASS, String sAPI_KEY, String sAPI_HASH, String sDOMAIN, String sPATH, String sQUERY, ReportFormat eREPORT)
        {
            API_HTTP_USER = sAPI_HTTP_USER;
            API_HTTP_PASS = sAPI_HTTP_PASS;
            API_KEY = sAPI_KEY;
            API_HASH = sAPI_HASH;
            DOMAIN = sDOMAIN;
            PATH = sPATH;
            REPORT = eREPORT;
            QUERY = sQUERY;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sQUERY"></param>
        /// <param name="eREPORT"></param>
        public ReportParams(String sQUERY, ReportFormat eREPORT)
        {
            API_HTTP_USER = @"user_http_RM3Qhj9cdT99";
            API_HTTP_PASS = @"Rmxhj9cdT9T9MT3C6Y8hj9cdT9Eq";
            API_KEY = @"yahj9cdT95e8wphj9cdT9881i";
            API_HASH = @"Yrxhj9cdT93VCpo0IzCZazUhj9cdT9jvnmU22tlwtPBhj9cdT9c2r";
            DOMAIN = @"https://secure.petro-logistics.com";
            PATH = "/api/movementsdata.php";

            REPORT = eREPORT;
            QUERY = sQUERY;
        }

        public ReportParams()
        {

        }

        public string API_HTTP_USER { get; set; }
        public string API_HTTP_PASS { get; set; }
        public string API_KEY { get; set; }
        public string API_HASH { get; set; }   
        public string DOMAIN { get; set; }
        public string PATH { get; set; }
        public ReportFormat REPORT { get; set; }
        public string QUERY { get; set; }
        public string url { get { return DOMAIN + PATH; } }

        public string postdata
        {
            get
            {
                //api_key=" + API_KEY + "&api_hash=" + API_HASH + "&format=csv&query_name=" + QUERY_NAME + "&csv_with_headers=1

                string BASE = @"api_key=" + API_KEY + @"&api_hash=" + API_HASH;

                switch (REPORT)
                {
                    case ReportFormat.csv:
                    {
                        return BASE + @"&format=csv&query_name=" + QUERY + @"&csv_with_headers=1";

                    }
                    case ReportFormat.json:
                    {
                        return BASE + @"&format=json&query_name=" + QUERY;
                    }
                    case ReportFormat.xml:
                    default:
                    {
                        return BASE + @"&format=xml&query_name=" + QUERY;
                    }
                }

            }
        }

        public NetworkCredential GetCredential()
        {
            return new NetworkCredential(API_HTTP_USER, API_HTTP_PASS);
        }
    }
    public class FetchReport
    {
        private readonly ReportParams _params;
        public FetchReport(ReportParams reportparams)
        {
            _params = reportparams;
        }

        public string getreport()
        {
            string errordata = "";
            string data = "";
            try
            {
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(_params.url);
                request.AuthenticationLevel = System.Net.Security.AuthenticationLevel.None;
                request.AutomaticDecompression = DecompressionMethods.None;

                //  CredentialCache myCache = new CredentialCache();

                //   myCache.Add( new Uri(_params.url), "Basic", );
                request.Credentials = _params.GetCredential();
                // Set some reasonable limits on resources used by this request
                request.MaximumAutomaticRedirections = 4;
                request.MaximumResponseHeadersLength = 4;
                request.Method = "POST";
                request.ContentType = "application/x-www-form-urlencoded";

                // create post data
                UTF8Encoding u8 = new UTF8Encoding();
                byte[] arrrequest = u8.GetBytes(_params.postdata.ToCharArray());
                request.ContentLength = arrrequest.Length;

                // send poast data 
                Stream requestStm = request.GetRequestStream();
                try
                {
                    requestStm.Write(arrrequest, 0, arrrequest.Length);
                }
                catch (Exception e)
                {
                    // catch fail when posting request data
                    errordata = e.Message;
                }
                finally
                {
                    requestStm.Close();
                }

                // get the response
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                StreamReader readStream = null;
                try
                {
                    /**PUT AN DELEGATE HERE TO RECEIVE STATUS MESSAGES */
                    // Console.WriteLine ("Content length is {0}", response.ContentLength);
                    // Console.WriteLine ("Content type is {0}", response.ContentType);

                    // Get the stream associated with the response.
                    Stream receiveStream = response.GetResponseStream();

                    // Pipes the stream to a higher level stream reader with the required encoding format. 
                    readStream = new StreamReader(receiveStream, Encoding.UTF8);
                    data = readStream.ReadToEnd();
                }
                catch (Exception e)
                {
                    // catch any responce fails
                    errordata += e.Message;
                }
                finally
                {
                    //Console.WriteLine ("Response stream received.");
                    //Console.WriteLine (readStream.ReadToEnd ().);
                    response.Close();
                    readStream.Close();
                }

            }
            catch (Exception e)
            {
                // catch any request fails
                errordata += e.Message;
            }
            finally
            {
                // nothing to do. 
            }

            if (string.IsNullOrEmpty(data)) return errordata;
            return data;
        }

    }
}
