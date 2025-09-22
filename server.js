const http = require('http');

// Sample predefined JSON array (100 records for example)
const data = Array.from({ length: 100000 }, (_, i) => ({
  id: i + 1,
  name: `Item${i + 1}`,
  value: Math.random().toFixed(4)
}));

// Async generator to yield CSV chunks from JSON array
async function* generateCsvChunks(jsonArray, chunkSize = 10) {
  let isFirstChunk = true;
  for (let i = 0; i < jsonArray.length; i += chunkSize) {
    // Take a chunk of the array
    const chunk = jsonArray.slice(i, i + chunkSize);
    // Convert to CSV
    let chunkString = '';
    if (isFirstChunk) {
      chunkString += 'id,name,value\n'; // Header for first chunk
      isFirstChunk = false;
    }
    chunkString += chunk
      .map(item => `${item.id || 'N/A'},${item.name || 'N/A'},${item.value || 'N/A'}`)
      .join('\n') + '\n';
    yield Buffer.from(chunkString); // Yield as Buffer for efficiency
    // Optional: Simulate real-time processing (remove if not needed)
    await new Promise(resolve => setTimeout(resolve, 100));
  }
}

const server = http.createServer((req, res) => {
  if (req.url === '/stream-csv' && req.method === 'GET') {
    // Set headers for CSV streaming
    res.writeHead(200, {
      'Content-Type': 'text/csv',
      'Transfer-Encoding': 'chunked',
      'Content-Disposition': 'attachment; filename="data.csv"'
    });

    // Use CSV generator
    const { Readable } = require('stream');
    const readable = Readable.from(generateCsvChunks(data, 10));
    readable.pipe(res);

    // Handle errors
    readable.on('error', (err) => {
      console.error('Stream error:', err);
      if (!res.headersSent) {
        res.statusCode = 500;
        res.end('Internal Server Error');
      }
    });
  } else {
    res.writeHead(404);
    res.end('Not Found');
  }
});

// Start server
server.listen(3000, () => {
  console.log('Server running at http://localhost:3000/stream-csv');
});