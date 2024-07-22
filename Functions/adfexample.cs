using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Azure.Storage.Blobs;
using FileFunction.Models;
using System.Text;
using System.Collections.Generic;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Extensions.FileProviders;
using System.Linq;

namespace FileFunction.Functions
{
    public static class adfexample
    {
        [FunctionName("adfexample")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
            [Blob("orders", Connection = "AzureWebJobsStorage")] BlobContainerClient container,
            ILogger log)
        {
            log.LogInformation("Processing a request");

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            var request = JsonConvert.DeserializeObject<FileRequest>(requestBody);
            var fileName = request.fileName;

            Console.WriteLine($"Received filename={fileName}");

            await container.CreateIfNotExistsAsync();

            BlobItem inputBlob = null;

            await foreach (BlobItem blob in container.GetBlobsAsync())
            {
                if (blob.Name.Equals(fileName, StringComparison.OrdinalIgnoreCase))
                {
                    inputBlob = blob;
                    break;
                }
            
            }

            Console.WriteLine($"Found Blob {inputBlob.Name}");
            var blobClient = container.GetBlockBlobClient(inputBlob.Name);
            FileStream fileStream = File.OpenWrite("tmp333.txt");
            await blobClient.DownloadToAsync(fileStream);
            Console.WriteLine("downloaded to tmp333.txt");
            fileStream.Close();
            FileStream readFileStream = File.OpenRead("tmp333.txt");

            StreamReader reader = new StreamReader(readFileStream);
            string content = reader.ReadToEnd();
            Console.WriteLine("Read the contents");
            Console.WriteLine(content);
            readFileStream.Close();

            string[] outputFiles = {"or.txt", "ce1.txt", "ce2.txt", "ce3.txt", "ce4.txt", "ce5.txt", "ce6.txt",
            "ce7.txt", "ce8.txt", "ce9.txt"};
            List<BlobClient> outputBlobs = new List<BlobClient>();
            
            List<string> orFileContent = new List<string>();
            List<string> ce1FileContent = new List<string>();
            List<string> ce2FileContent = new List<string>();
            List<string> ce3FileContent = new List<string>();
            List<string> ce4FileContent = new List<string>();
            List<string> ce5FileContent = new List<string>();
            List<string> ce6FileContent = new List<string>();
            List<string> ce7FileContent = new List<string>();
            List<string> ce8FileContent = new List<string>();
            List<string> ce9FileContent = new List<string>();
            
            foreach (string file in outputFiles)
            {
                outputBlobs.Add(container.GetBlobClient(file));
            }

            //var outputBlob = container.GetBlobClient($"{inputBlob.Name}_{DateTime.UtcNow:yyyy_MM_dd_HH_mm_ss}.txt");

            Console.WriteLine("Uploading the contents");
            foreach (string line in content.Split(new char[] { '\r', '\n' }))
            {
                Console.WriteLine($"line: {line}");
                if (line == null || line.Length == 0) continue;
                //String itemId, itemName, itemQuantity, itemPrice, itemTotal;
                String[] columns = line.Split(new char[] { ',' });
                if (columns.Length == 0) continue;
                if (columns[0][0] == 'O' && columns[0][1]=='R')
                {
                    orFileContent.Add(getColumns(columns));

                } else if (columns[0][0] == 'C' && columns[0][1] == 'E')
                {
                    switch (columns[3][0])
                    {
                        case '1': ce1FileContent.Add(getColumns(columns)); break;
                        case '2': ce2FileContent.Add(getColumns(columns)); break;
                        case '3': ce3FileContent.Add(getColumns(columns)); break;
                        case '4': ce4FileContent.Add(getColumns(columns)); break;
                        case '5': ce5FileContent.Add(getColumns(columns)); break;
                        case '6': ce6FileContent.Add(getColumns(columns)); break;
                        case '7': ce7FileContent.Add(getColumns(columns)); break;
                        case '8': ce8FileContent.Add(getColumns(columns)); break;
                        case '9': ce9FileContent.Add(getColumns(columns)); break;
                        default: break;
                    }
                }
                
                
            }

            Console.WriteLine("Uploading the contents OR");

            using var ORoutput = new MemoryStream(Encoding.UTF8.GetBytes(String.Join("\n", orFileContent)));
            await outputBlobs[0].UploadAsync(ORoutput);
            Console.WriteLine("Uploading the contents CE1");
            using var CE1output = new MemoryStream(Encoding.UTF8.GetBytes(String.Join("\n", ce1FileContent)));
            await outputBlobs[1].UploadAsync(CE1output);

            Console.WriteLine("Uploading the contents CE2");
            using var CE2output = new MemoryStream(Encoding.UTF8.GetBytes(String.Join("\n", ce2FileContent)));
            await outputBlobs[2].UploadAsync(CE2output);

            Console.WriteLine("Uploading the contents CE3");
            using var CE3output = new MemoryStream(Encoding.UTF8.GetBytes(String.Join("\n", ce3FileContent)));
            await outputBlobs[3].UploadAsync(CE3output);

            Console.WriteLine("Uploading the contents CE4");
            using var CE4output = new MemoryStream(Encoding.UTF8.GetBytes(String.Join("\n", ce4FileContent)));
            await outputBlobs[4].UploadAsync(CE4output);

            Console.WriteLine("Uploading the contents CE5");
            using var CE5output = new MemoryStream(Encoding.UTF8.GetBytes(String.Join("\n", ce5FileContent)));
            await outputBlobs[5].UploadAsync(CE5output);

            Console.WriteLine("Uploading the contents CE6");
            using var CE6output = new MemoryStream(Encoding.UTF8.GetBytes(String.Join("\n", ce6FileContent)));
            await outputBlobs[6].UploadAsync(CE6output);

            Console.WriteLine("Uploading the contents CE7");
            using var CE7output = new MemoryStream(Encoding.UTF8.GetBytes(String.Join("\n", ce7FileContent)));
            await outputBlobs[7].UploadAsync(CE7output);

            Console.WriteLine("Uploading the contents CE8");
            using var CE8output = new MemoryStream(Encoding.UTF8.GetBytes(String.Join("\n", ce8FileContent)));
            await outputBlobs[8].UploadAsync(CE8output);

            Console.WriteLine("Uploading the contents CE9");
            using var CE9output = new MemoryStream(Encoding.UTF8.GetBytes(String.Join("\n", ce9FileContent)));
            await outputBlobs[9].UploadAsync(CE9output);


            return new OkObjectResult(inputBlob.Name);
        }

        public static string getColumns(string[] columns)
        {
            String retString = columns[1];
            for (int i = 2; i < columns.Length; i++)
            {
                retString = String.Join(",", retString, columns[i]);
            }
            return retString;


        }
    }
}
