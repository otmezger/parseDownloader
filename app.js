var _ = require('underscore');
var Parse = require('parse/node').Parse;
var fs = require('fs');
var config = require('./config');

// define a location. All sensors from all this machines will be downloaded.
var myLocationID  = '1U97q8qaPi';


// overwrite and limit to only this machines. leave empty if not needed.
var limitToThisMachines = [
  //'S7OVisnVy4',
  //'K3YeIg1kza',
];

// limit to only this sensor type.
var getThisSensorTypes = [
  '9Gy9tDuCaq'
];

var prefix = {
  location:'exportLocation_',
  file:'exportMachine_',
}

Parse.initialize(config.appKey, config.jsKey,config.masterKey);
Parse.serverURL = config.serverURL;
Parse.Cloud.useMasterKey();
// ---------------------------------------------------------------------------------- machines
// --------------------------------------------------- MODEL
var MachineModel = Parse.Object.extend({
  className: "Machine"
});

// --------------------------------------------------- QUERRY
var MachineQuery = new Parse.Query(MachineModel);
MachineQuery.include("Location");

// ---------------------------------------------------------------------------------- Location
// --------------------------------------------------- MODEL
var LocationModel = Parse.Object.extend({
  className: "Location"
});
// ---------------------------------------------------------------------------------- Sensor
// --------------------------------------------------- MODEL
var SensorModel = Parse.Object.extend({
  className: "Sensors"
});
// --------------------------------------------------- QUERY
var SensorQuery = new Parse.Query(SensorModel);
SensorQuery.include('SensorType');
SensorQuery.include('Machine');
//SensorQuery.find();
//SensorQuery.equalTo('id',"ehBVWff7D0");
//var mySensorsIDs= ['ehBVWff7D0'];

// ---------------------------------------------------------------------------------- DataPointi
// --------------------------------------------------- MODEL
var DataPointModel = Parse.Object.extend({
  className: "DataPoint"
});
// --------------------------------------------------- QUERY
var DataPointQueryConfig = {
  maxModels: 10000, // the max amount of models we should get
  currentSkip: 0, // the current skip for this collection
  limit: 1000 // the limit per query.
}
if (DataPointQueryConfig.limit > DataPointQueryConfig.maxModels){console.error('error in DataPointQueryConfig. Limit must be less than maxModels');}
var DataPointQuery = new Parse.Query(DataPointModel);
//DataPointQuery.find();
DataPointQuery.limit(DataPointQueryConfig.limit);
DataPointQuery.ascending('timeStamp');
var DataPointArray = [];

// ---------------------------------------------------------------------------------- END OF  dataPointCollection

var grandResults = {};
var machinesResults;
var machineONOFFResults;
var R;



var thisLocation = new LocationModel();
thisLocation.id = myLocationID;
console.log('--- set up location ' + myLocationID);
MachineQuery.equalTo('Location',thisLocation);
if(limitToThisMachines.length >0){
  MachineQuery.containedIn('objectId',limitToThisMachines);
}
MachineQuery.find().then(function(machinesResults_){
  machinesResults = machinesResults_; // make variable globally accesible
  console.log('--- got ' + machinesResults.length + ' machines at this location');

  // OK, now we will loop over the machines.
  console.log('--- Starting to loop over the Machines');
  var machinePromise = Parse.Promise.as();
  _.each(machinesResults,function(thisMachine){
      machinePromise = machinePromise.then(function(){
        R = {}; // results for this machine!
        R.machine = thisMachine;
        R.sensors = {};
        //R.machine.id = thisMachine.id;
        console.log('--- --- Startint with machine ' + thisMachine.get('Name') + ' (' + thisMachine.id +')');
        //console.log(thisMachine);
        var MachineONOFFQuery = new Parse.Query('MachineOnOff');
        MachineONOFFQuery.descending('eventStartDate');
        MachineONOFFQuery.equalTo('Machine',thisMachine);
        //console.log(MachineONOFFQuery);
        return MachineONOFFQuery.first();
      }).then(function(thisMachineONOFF){

        if (thisMachineONOFF){
            console.log('--- --- --- got machine ON OFF last state = ' + thisMachineONOFF.get('on'));
            R.machineONOFF = thisMachineONOFF.attributes;
        }else{
          console.log('--- --- --- got machine ON OFF empty. setting OFF');
          R.machineONOFF = {'on':false};
        }



        var getThisSensorTypesObjects = [];
        _.each(getThisSensorTypes,function(thisSensorTypeID){
          var sensorTypeObject = new Parse.Object('SensorType');
          sensorTypeObject.id = thisSensorTypeID;
          getThisSensorTypesObjects.push(sensorTypeObject);
        });
        //console.log(machinesResults);
        //console.log(getThisSensorTypesObjects);
        SensorQuery.equalTo('Machine',R.machine);
        SensorQuery.containedIn('SensorType',getThisSensorTypesObjects);
        return SensorQuery.find();

      },function(error){
        console.log('--- --- === Error in machineONOFF ');
        console.log(error);
      }).then(function(sensors){
        console.log('--- --- --- got ' + sensors.length + ' sensors for this Machine. Going to iterate');
        var dataPromise = Parse.Promise.as();
        _.each(sensors,function(thisSensor){
          dataPromise = dataPromise.then(function(){
            thisSensorId = thisSensor.id;
            console.log('--- --- --- --- working on sensor with id ' + thisSensorId);

            R.sensors['sensor_'+thisSensorId] = {};
            R.sensors['sensor_'+thisSensorId].meta = thisSensor;
            R.sensors['sensor_'+thisSensorId].data = {};

            DataPointQuery.equalTo('Sensor',thisSensor);
            //console.log(R.machineONOFF.eventStartDate);
            //R.machineONOFF.eventStartDate = new Date('2016-02-18 00:00');
            //console.log(R.machineONOFF.eventStartDate);
            if (R.machineONOFF.eventStartDate){
              console.log('--- --- --- --- --- limiting datapoint query to timestamp:' + R.machineONOFF.eventStartDate);
              //DataPointQuery.greaterThanOrEqualTo('timeStamp',new Date('2016-02-18 00:00'));
              DataPointQuery.greaterThanOrEqualTo('timeStamp',R.machineONOFF.eventStartDate);

              //console.log(DataPointQuery);

            }else{
              console.log('--- --- --- --- --- not limiting datapoint query!');
            }
            DataPointQuery.skip(0); // reset skip
            //DataPointQuery.limit(DataPointQueryConfig.limit)
            DataPointArray = [];
            //console.log(DataPointQuery);
            paginatedFetchPromise = DataPointQuery.find()
            /*.then(function(result){
              console.log(result);
              return Parse.Promise.as(result);
            },function(error){
              console.log(error);
              return Parse.Promise.error();
            });*/
            for (var i = DataPointQueryConfig.limit; i<=DataPointQueryConfig.maxModels; i = i + DataPointQueryConfig.limit){
              //console.log(i);
              paginatedFetchPromise = paginatedFetchPromise.then(function(thisDataPoints){
                if (thisDataPoints){
                   //console.log('--- --- --- --- --- got ' + thisDataPoints.length + ' datapoints');
                }else{
                  //console.log('--- --- --- --- === got 0 datapoints :-(');
                }
                _.each(thisDataPoints,function(thisDataPoint){
                   DataPointArray.push(thisDataPoint);
                });
                DataPointQuery.skip(DataPointArray.length);
                if (thisDataPoints){
                  if (thisDataPoints.length == DataPointQueryConfig.limit ){
                    // let's do it again!
                     return DataPointQuery.find();
                   } else{
                     // we are done
                     return Parse.Promise.as();
                   }
                }else{
                  // we are done
                  return Parse.Promise.as();
                }
              });// tail of paginatedFetchPromise
            }
            //return DataPointQuery.find();
            return paginatedFetchPromise;


          },function(error){
            console.log('--- --- --- === got an error making the DataPointQuery for sensor with id ' + thisSensor.id);
            console.log(error);
          }).then(function(){
            console.log('--- --- --- --- --- got ' + DataPointArray.length + ' DataPoints for SensorID: ' + thisSensorId);
            R.sensors['sensor_'+thisSensorId].data = DataPointArray;
            return Parse.Promise.as();
          },function(error){
            console.log('--- --- --- === got an error in paginatedFetchPromise');
            console.log(error);
          }); // tail of data dataPromise
          return dataPromise;
        });//end of each sensor

        return dataPromise;
      },function(error){
        console.log('--- --- === Error in sensors');
        console.log(error);
      }).then(function(){
        //console.log('--- --- Attempting to export file for machine');

        // now this machine has no more data.
        var myString = JSON.stringify(R);
        var myStringEscaped = myString.replace(/\\n/g, "\\n")
          .replace(/\\'/g, "\\'")
          .replace(/\\"/g, '\\"')
          .replace(/\\&/g, "\\&")
          .replace(/\\r/g, "\\r")
          .replace(/\\t/g, "\\t")
          .replace(/\\b/g, "\\b")
          .replace(/\\f/g, "\\f");

        // let's check if the directory exists

        var machineExportDir = './exportParse/' +prefix.location + myLocationID;
        try {
            fs.accessSync(machineExportDir, fs.F_OK);
            // this directory exist!
        } catch (e) {
            // the directory does not exist, create.
            fs.mkdirSync(machineExportDir)
        }
        var machineExportFile = machineExportDir + '/' + prefix.file + R.machine.id +'.json';
        fs.writeFileSync(machineExportFile, myStringEscaped , 'utf-8');
        console.log('--- --- Saved file for machine ' + machineExportFile);
        return Parse.Promise.as();
      }); // tail of machinePromise
      return machinePromise;

  }); // end of looping over machines.
  return machinePromise;
}).then(function(){
  console.log('done');
},function(error){
  console.log('error');
  console.log(error);
});// promise tail
