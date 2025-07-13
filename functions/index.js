const functions = require('firebase-functions');
const { Firestore } = require('@google-cloud/firestore');
const { Storage } = require('@google-cloud/storage');
const csv = require('csv-parser');
const { Readable } = require('stream');

const firestore = new Firestore();
const storage = new Storage();

const bucketName = 'stockdashboard-dea1d.firebasestorage.app';
const today = new Date();
const mm = String(today.getMonth() + 1).padStart(2, '0');
const dd = String(today.getDate()).padStart(2, '0');
const yyyy = today.getFullYear();

const formattedDate = dd + mm + yyyy;
const fileName = `sec_bhavdata_full_${formattedDate}.csv`;

console.log('filename::', fileName);

exports.importBhavcopy = functions.https.onRequest(async (req, res) => {
  try {
    const file = storage.bucket(bucketName).file(fileName);
    const [contents] = await file.download();

    const stream = Readable.from(contents.toString());
    const results = [];

    await new Promise((resolve, reject) => {
      stream
        .pipe(csv({ trim: true, mapHeaders: ({ header }) => header.trim() }))
        .on('data', (data) => {
          if (!data['SYMBOL']) return;

          results.push({
            symbol: data['SYMBOL'],
            series: data['SERIES'],
            open: parseFloat(data['OPEN_PRICE']),
            high: parseFloat(data['HIGH_PRICE']),
            low: parseFloat(data['LOW_PRICE']),
            close: parseFloat(data['CLOSE_PRICE']),
          });
        })
        .on('end', resolve)
        .on('error', reject);
    });

    const batch = firestore.batch();
    results.forEach((item) => {
      const docRef = firestore.collection('nse_bhavcopy').doc(item.symbol);
      batch.set(docRef, item);
    });

    await batch.commit();
    res.status(200).send(`✅ Saved ${results.length} records to Firestore.`);
  } catch (err) {
    console.error('❌ Error:', err);
    res.status(500).send('❌ Error processing CSV.');
  }
});
