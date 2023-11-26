const { Readable } = require('stream');
const fs = require('fs').promises;
const parseString = require('xml-js');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

async function readXMLFile(filename) {
  const xmlData = await fs.readFile(filename, 'utf-8');
  const jsonData = parseString.xml2json(xmlData, { compact: true, spaces: 4 });
  return JSON.parse(jsonData).records.record;
}

async function readCSVFile(filename) {
  const data = [];
  try {
    const fileData = await fs.readFile(filename, 'utf-8');
    return new Promise((resolve, reject) => {
      const stream = Readable.from(fileData); // Creating a stream from file data
      stream
        .pipe(csv())
        .on('data', (row) => {
          data.push(row);
        })
        .on('end', () => {
          resolve(data);
        })
        .on('error', (err) => {
          reject(err);
        });
    });
  } catch (err) {
    throw new Error(`Error reading file ${filename}: ${err.message}`);
  }
}

async function processTransactions(xmlTransactions, csvTransactions) {
  const referenceMap = new Map();
  const failedTransactions = [];

  for (const transaction of xmlTransactions) {
    const {
      reference,
      accountNumber,
      mutation,
      description,
      startBalance,
      endBalance: transactionEndBalance,
    } = transaction._attributes;

    const parsedStartBalance = parseFloat(startBalance);
    const parsedMutation = parseFloat(mutation);
    const parsedTransactionEndBalance = parseFloat(transactionEndBalance);

    const expectedEndBalance = parsedStartBalance + parsedMutation;

    if (
      referenceMap.has(reference) ||
      parsedTransactionEndBalance !== expectedEndBalance
    ) {
      failedTransactions.push({
        Reference: reference,
        'Account Number': transaction.accountNumber._text,
        Description: transaction.description._text,
        'Start Balance': transaction.startBalance._text,
        Mutation: transaction.mutation._text,
        'End Balance': transaction.endBalance._text,
      });
    } else {
      referenceMap.set(reference, true);
    }

    const csvTransaction = csvTransactions.find(
      (csvTransaction) => csvTransaction.Reference === reference
    );

    if (!csvTransaction) {
      failedTransactions.push({
        Reference: reference,
        'Account Number': transaction.accountNumber._text,
        Description: transaction.description._text,
        'Start Balance': transaction.startBalance._text,
        Mutation: transaction.mutation._text,
        'End Balance': transaction.endBalance._text,
      });
    }
  }
  return failedTransactions;
}

async function start() {
  try {
    const xmlTransactions = await readXMLFile('transactions.xml');
    const csvTransactions = await readCSVFile('transactions.csv');

    const failedTransactions = await processTransactions(
      xmlTransactions,
      csvTransactions
    );

    await fs.writeFile(
      'failed_transactions_report.csv',
      JSON.stringify(failedTransactions, null, 4)
    );

    if (failedTransactions.length === 0) {
      console.log('All transactions have been verified.');
    } else {
      console.log(
        'Failed transactions detected. See the report: failed_transactions_report.csv'
      );
    }

    const csvWriter = createCsvWriter({
      path: 'failedTransactions.csv',
      header: [
        { id: 'Reference', title: 'Reference' },
        { id: 'Account Number', title: 'Account Number' },
        { id: 'Description', title: 'Description' },
        { id: 'Start Balance', title: 'Start Balance' },
        { id: 'Mutation', title: 'Mutation' },
        { id: 'End Balance', title: 'End Balance' },
      ],
      alwaysQuote: true,
    });

    await csvWriter.writeRecords(failedTransactions);
    console.log('...Done');
  } catch (err) {
    console.error('Error:', err);
  }
}

const express = require('express');
const app = express();

app.get('/', async function (req, res) {
  try {
    const data = await fs.readFile('failed_transactions_report.csv', 'utf-8');
    res.set('Content-Type', 'text/csv');
    res.send(data);
  } catch (err) {
    res.status(404).send('Error: File not found');
  }
});

app.listen(8080, function () {
  console.log('server running on port 8080!');
});

// Starting transaction processing
start();
