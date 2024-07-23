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
            ExecutionContext executionContext,
            ILogger log)
        {
            try
            {


                log.LogInformation("Processing a request");

                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                var request = JsonConvert.DeserializeObject<FileRequest>(requestBody);
                var fileName = request.fileName;

                log.LogInformation($"Received filename={fileName}");

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

                log.LogInformation($"Found Blob {inputBlob.Name}");
                var blobClient = container.GetBlockBlobClient(inputBlob.Name);


                //var path = System.IO.Path.Combine(executionContext.FunctionDirectory, "tmp333.txt");
                var path = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

                //FileStream fileStream = File.OpenWrite("c:\\tmp333.txt");
                
                using (FileStream fs = File.Create(path))
                {
                    await blobClient.DownloadToAsync(fs);
                    log.LogInformation($"downloaded to {path}");
                    fs.Close();
                }


                FileStream readFileStream = File.OpenRead(path);

                StreamReader reader = new StreamReader(readFileStream);
                string content = reader.ReadToEnd();
                log.LogInformation("Read the contents");
                log.LogInformation(content);
                readFileStream.Close();

                string[] outputFiles = {
                    $"or_{DateTime.UtcNow:yyyy_MM_dd_HH_mm_ss}.txt",
                    $"ce1_{DateTime.UtcNow:yyyy_MM_dd_HH_mm_ss}.txt",
                    $"ce2_{DateTime.UtcNow:yyyy_MM_dd_HH_mm_ss}.txt",
                    $"ce3_{DateTime.UtcNow:yyyy_MM_dd_HH_mm_ss}.txt",
                    $"ce4_{DateTime.UtcNow:yyyy_MM_dd_HH_mm_ss}.txt",
                    $"ce5_{DateTime.UtcNow:yyyy_MM_dd_HH_mm_ss}.txt",
                    $"ce6_{DateTime.UtcNow:yyyy_MM_dd_HH_mm_ss}.txt",
                    $"ce7_{DateTime.UtcNow:yyyy_MM_dd_HH_mm_ss}.txt",
                    $"ce8_{DateTime.UtcNow:yyyy_MM_dd_HH_mm_ss}.txt",
                    $"ce9_{DateTime.UtcNow:yyyy_MM_dd_HH_mm_ss}.txt"
                };
                 
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

                log.LogInformation("Uploading the contents");
                foreach (string line in content.Split(new char[] { '\r', '\n' }))
                {
                    log.LogInformation($"line: {line}");
                    if (line == null || line.Length == 0) continue;
                    //String itemId, itemName, itemQuantity, itemPrice, itemTotal;
                    String[] columns = line.Split(new char[] { ',' });
                    if (columns.Length == 0) continue;
                    if (columns[0][0] == 'O' && columns[0][1] == 'R')
                    {
                        orFileContent.Add(getColumns(columns));

                    }
                    else if (columns[0][0] == 'C' && columns[0][1] == 'E')
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


                List<string> generatedFiles = new List<string>();

                if (orFileContent.Count > 0)
                {
                    log.LogInformation("Uploading the contents OR");
                    using var ORoutput = new MemoryStream(Encoding.UTF8.GetBytes(String.Join("\n", orFileContent)));
                    await outputBlobs[0].UploadAsync(ORoutput);
                    generatedFiles.Add(outputFiles[0]);
                }

                if (ce1FileContent.Count > 0)
                {
                    log.LogInformation("Uploading the contents CE1");
                    using var CE1output = new MemoryStream(Encoding.UTF8.GetBytes(String.Join("\n", ce1FileContent)));
                    await outputBlobs[1].UploadAsync(CE1output);
                    generatedFiles.Add(outputFiles[1]);
                }

                if (ce2FileContent.Count > 0)
                {
                    log.LogInformation("Uploading the contents CE2");
                    using var CE2output = new MemoryStream(Encoding.UTF8.GetBytes(String.Join("\n", ce2FileContent)));
                    await outputBlobs[2].UploadAsync(CE2output);
                    generatedFiles.Add(outputFiles[2]);

                }

                if (ce3FileContent.Count > 0)
                {
                    log.LogInformation("Uploading the contents CE3");

                    using var CE3output = new MemoryStream(Encoding.UTF8.GetBytes(String.Join("\n", ce3FileContent)));
                    await outputBlobs[3].UploadAsync(CE3output);
                    generatedFiles.Add(outputFiles[3]);

                }

                if (ce4FileContent.Count > 0)
                {
                    log.LogInformation("Uploading the contents CE4");
                    using var CE4output = new MemoryStream(Encoding.UTF8.GetBytes(String.Join("\n", ce4FileContent)));
                    await outputBlobs[4].UploadAsync(CE4output);
                    generatedFiles.Add(outputFiles[4]);

                }

                if (ce5FileContent.Count > 0)
                {
                    log.LogInformation("Uploading the contents CE5");
                    using var CE5output = new MemoryStream(Encoding.UTF8.GetBytes(String.Join("\n", ce5FileContent)));
                    await outputBlobs[5].UploadAsync(CE5output);
                    generatedFiles.Add(outputFiles[5]);

                }

                if (ce6FileContent.Count > 0)
                {
                    log.LogInformation("Uploading the contents CE6");
                    using var CE6output = new MemoryStream(Encoding.UTF8.GetBytes(String.Join("\n", ce6FileContent)));
                    await outputBlobs[6].UploadAsync(CE6output);
                    generatedFiles.Add(outputFiles[6]);

                }

                if (ce7FileContent.Count > 0)
                {
                    log.LogInformation("Uploading the contents CE7");
                    using var CE7output = new MemoryStream(Encoding.UTF8.GetBytes(String.Join("\n", ce7FileContent)));
                    await outputBlobs[7].UploadAsync(CE7output);
                    generatedFiles.Add(outputFiles[7]);

                }

                if (ce8FileContent.Count > 0)
                {
                    log.LogInformation("Uploading the contents CE8");
                    using var CE8output = new MemoryStream(Encoding.UTF8.GetBytes(String.Join("\n", ce8FileContent)));
                    await outputBlobs[8].UploadAsync(CE8output);
                    generatedFiles.Add(outputFiles[8]);

                }

                if (ce9FileContent.Count > 0)
                {
                    log.LogInformation("Uploading the contents CE9");
                    using var CE9output = new MemoryStream(Encoding.UTF8.GetBytes(String.Join("\n", ce9FileContent)));
                    await outputBlobs[9].UploadAsync(CE9output);
                    generatedFiles.Add(outputFiles[9]);

                }

                log.LogInformation($"Sending email for the files {String.Join("\n", generatedFiles)}");
                sendEmail(generatedFiles);
                return new OkObjectResult(inputBlob.Name);
            }
            catch (Exception ex)
            {
                log.LogError(ex.ToString());
            }
            return new OkObjectResult("");
        }

        public static void sendEmail(List<string> fileNames)
        {
            string files = String.Join("\n", fileNames);
            //log.LogInformation($"sending email for the generated files: {files}");

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
