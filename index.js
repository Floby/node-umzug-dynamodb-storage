const Joi = require('joi');
const delay = require('delay');

module.exports = DynamoDbStorage;

function DynamoDbStorage({ dynamodb, table = 'migrations' }) {
  const dynogels = require('dynogels-promisified');
  dynogels.dynamoDriver(dynamodb);

  const Migrations = dynogels.define(table, {
    hashKey: 'name',
    timestamps: true,
    schema: {
      name: Joi.string().required(),
      executed: Joi.boolean().default(false)
    }
  });
  delete dynogels.models[table]; // otherwise dynogels.createAll/deleteAll messes up

  this.logMigration = (migrationName) => {
    return ensureMigrationTable().then(() => {
      return Migrations.createAsync({
        name: migrationName,
        executed: true
      });
    });
  };

  this.unlogMigration = (migrationName) => {
    return ensureMigrationTable().then(() => {
      return Migrations.destroyAsync(migrationName);
    });
  };

  this.executed = () => {
    return ensureMigrationTable()
      .then(() => {
        return Migrations.scan()
          .where('executed')
          .equals(true)
          .loadAll()
          .execAsync();
      })
      .then(({ Items }) => {
        return Items.map((item) => item.get('name')).sort();
      });
  };

  function ensureMigrationTable() {
    return Migrations.describeTableAsync()
      .then((description) => {
        return description
          ? Migrations.updateTableAsync
          : Migrations.createTableAsync();
      })
      .catch(() => {
        return Migrations.createTableAsync();
      })
      .then(() => {
        return checkActive();
        function checkActive() {
          return Migrations.describeTableAsync().then(({ Table }) => {
            if (Table.TableStatus !== 'ACTIVE') {
              return delay(500).then(checkActive);
            }
          });
        }
      });
  }
}
