const http = require('http');
const yazl = require('yazl');

// Sample predefined JSON array (100 records)
const data = Array.from({ length: 100000 }, (_, i) => ({
  id: i + 1,
  name: `Item${i + 1}`,
  value: Math.random().toFixed(4)
}));

// Async generator to stream ZIP with CSV files
async function* generateZipStream(jsonArray, recordsPerFile = 25) {
  const zipfile = new yazl.ZipFile();

  // Create a readable stream for the ZIP
  const zipStream = zipfile.outputStream;

  // Add CSV files to ZIP
  for (let i = 0; i < jsonArray.length; i += recordsPerFile) {
    const chunk = jsonArray.slice(i, i + recordsPerFile);
    let csvContent = 'id,name,value\n';
    csvContent += chunk
      .map(item => `${item.id || 'N/A'},${item.name || 'N/A'},${item.value || 'N/A'}`)
      .join('\n') + '\n';
    
    zipfile.addBuffer(Buffer.from(csvContent), `data_part_${Math.floor(i / recordsPerFile) + 1}.csv`);
  }

  // Signal end of ZIP
  zipfile.end();

  // Yield chunks from the ZIP stream
  for await (const chunk of zipStream) {
    yield chunk;
    await new Promise(resolve => setTimeout(resolve, 10)); // Optional delay
  }
}

const server = http.createServer(async (req, res) => {
  if (req.url === '/stream-csv-zip' && req.method === 'GET') {
    try {
      res.writeHead(200, {
        'Content-Type': 'application/zip',
        'Transfer-Encoding': 'chunked',
        'Content-Disposition': 'attachment; filename="data_files.zip"'
      });

      const { Readable } = require('stream');
      const readable = Readable.from(generateZipStream(data, 1000));
      readable.pipe(res);

      readable.on('error', (err) => {
        console.error('Stream error:', err);
        if (!res.headersSent) {
          res.statusCode = 500;
          res.end('Internal Server Error');
        }
      });
    } catch (err) {
      console.error('Server error:', err);
      res.statusCode = 500;
      res.end('Internal Server Error');
    }
  } else {
    res.writeHead(404);
    res.end('Not Found');
  }
});

server.listen(3000, () => {
  console.log('Server running at http://localhost:3000/stream-csv-zip');
});