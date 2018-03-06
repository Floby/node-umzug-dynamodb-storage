const dynalite = require('dynalite');
const DynamoDbStorage = require('../');
const AWS = require('aws-sdk');
const pify = require('pify');
const { expect } = require('chai')

describe('new DynamoDbStorage({ dynamodb, table })', function () {
  this.timeout(20000)
  const migrationName = '0125678-some-migration.js';
  const tableName = 'some-migrations';
  let dynamoDbStorage;

  const dynamodb = withDynalite(1000);

  beforeEach(() => {
    dynamoDbStorage = new DynamoDbStorage({ dynamodb, table: tableName });
  });

  it('returns an instance of DynamoDbStorage', () => {
    expect(dynamoDbStorage).to.be.an.instanceOf(DynamoDbStorage);
  });

  describe('.logMigration(name)', () => {
    it('inserts a row for this migration', () => {
      // Given
      const getItem = pify(dynamodb.getItem.bind(dynamodb));
      // When
      return dynamoDbStorage.logMigration(migrationName).then(() => {
        // Then
        return getItem({
          TableName: tableName,
          Key: {
            name: { S: migrationName }
          }
        }).then(({ Item }) => {
          expect(Item.name.S).to.equal(migrationName);
          expect(Item.executed.BOOL).to.equal(true);
        });
      });
    });
  });

  describe('.unlogMigration(name)', () => {
    beforeEach(() => {
      return dynamoDbStorage.logMigration(migrationName);
    });

    it('remove the row for this migration', () => {
      // Given
      const getItem = pify(dynamodb.getItem.bind(dynamodb));
      // When
      return dynamoDbStorage.unlogMigration(migrationName).then(() => {
        // Then
        return getItem({
          TableName: tableName,
          Key: {
            name: { S: migrationName }
          }
        }).then(({ Item }) => {
          expect(Item).to.equal(undefined);
        });
      });
    });
  });

  describe('.executed()', () => {
    describe('when there were no migrations', () => {
      it('resolves []', () => {
        // When
        return dynamoDbStorage.executed().then((actual) => {
          // Then
          expect(actual).to.deep.equal([]);
        });
      });
    });

    describe('when there were successful migrations', () => {
      beforeEach(() => dynamoDbStorage.logMigration('1234-hello.js'));
      beforeEach(() => dynamoDbStorage.logMigration('5678-goodbye.js'));
      it('lists all passed migrations', () => {
        // When
        return dynamoDbStorage.executed().then((actual) => {
          // Then
          expect(actual).to.deep.equal(['1234-hello.js', '5678-goodbye.js']);
        });
      });
    });
  });
});

function withDynalite(createTableMs = 10) {
  const portNumber = 5252
  const dynaliteServer = dynalite({
    createTableMs
  });

  beforeEach((done) => {
    dynaliteServer.listen(portNumber, done)
  });

  afterEach((done) => {
    dynaliteServer.close(done)
  });

  const dynamodb = new AWS.DynamoDB({
    accessKeyId: 'hello',
    secretAccessKey: 'world',
    endpoint: 'http://localhost:' + portNumber,
    region: 'eu-west-1'
  });

  return dynamodb;
}
