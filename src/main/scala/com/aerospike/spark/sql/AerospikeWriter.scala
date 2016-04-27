package com.aerospike.spark.sql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.Logging
import scala.collection.JavaConversions._
import com.aerospike.spark.sql.AerospikeConfig._


object AerospikeWriter {

  def prepareDatabase(schema: StructType, mode: SaveMode, config: AerospikeConfig) : Boolean = {
    
    val targetSet = config.get(AerospikeConfig.SetName)
    val targetNamespace = config.get(AerospikeConfig.NameSpace)
    val updateByKey = config.get(AerospikeConfig.UpdateByKey)
    
    if(updateByKey == null){
      // The update modes are mutually exclusive
      sys.error(s"Both UpdateByOid and UpdateByValue have been specified, only one is allowed")
    }
    
    val client = AerospikeConnection.getClient(config);
    
    scope (new TransactionScope(TransactionMode.READ_UPDATE, purpose, TransactionScopeOption.REQUIRED)) to { tx =>
      var targetClass = com.objy.data.Class.lookupClass(targetClassName)
      var instanceData = null.asInstanceOf[Variable];

      // If the target class exists, get an iterator for existing data, otherwise create class
      if(targetClass != null){
        val fromBuilder = new OperatorExpressionBuilder("FROM").addLiteral(new Variable(targetClass))
        val exprTree = new ExpressionTreeBuilder(fromBuilder.build()).build("DO", ExpressionSetupOptions.NONE)
        instanceData = new Statement(exprTree).execute()
      } else {
        ULBSchemaProvider.createClass(schema, targetClassName)
      }

      // If data exists, consult mode to determine action
      val notUpdating = updateByOid.isEmpty() && updateByValue.isEmpty()
      if(instanceData != null){
        val dataIterator = instanceData.sequenceValue().iterator()
        if(dataIterator.hasNext()){
          mode match {
            case SaveMode.ErrorIfExists => sys.error(s"Data of type $targetClassName already exists")
            case SaveMode.Overwrite => {
              if(notUpdating){
                logDebug(s"Deleting all instances of type $targetClassName")
                deleteAllInstances(dataIterator)
              }
            }
            case SaveMode.Ignore => return false
            case _ => // keep going
          }
        }
      }
      tx.complete()   
    }
    
    return true 
  }
  
  private def deleteAllInstances(dataIterator: java.util.Iterator[Variable]){
    var count = 0;
    while(dataIterator.hasNext) {
      count += 1
      dataIterator.next().instanceValue().delete()
    }
    logDebug(s"Deleted $count instances")
  }
  
  
  def writeDataFrame(data: DataFrame, mode: SaveMode, config: ObjyConfig){
    val schema = data.schema
    data.foreachPartition { iterator =>
        writePartition(iterator, schema, mode, config) }
  }
  
  private def writePartition(iterator: Iterator[Row],
      schema: StructType, mode: SaveMode, config: ObjyConfig): Unit = {  
    
    // avoid creating sessions for empty partitions
    if(iterator.hasNext){
      val purpose = config.getIfNotEmpty(SessionPurpose, DEFAULT_WRITE_PURPOSE);
      ULBConnection.ensureConnection(config).makeCurrent()    
      scope (new TransactionScope(TransactionMode.READ_UPDATE, purpose, TransactionScopeOption.REQUIRED)) to { tx =>
        logDebug("begin write session for saving partition")
        var counter = 0
        
        // Setup methods for creating/finding objects depending on update modes
        // and compute conversion functions for each row field
        val updateByValueField = config.get(UpdateByValue)
        val isUpdating = !(config.get(UpdateByOid).isEmpty() && updateByValueField.isEmpty())
        val appendArrays = isUpdating && mode.equals( SaveMode.Append)
        val targetClass = com.objy.data.Class.lookupClass(config.get(DataClassName)) 
        val instanceFactory = getInstanceFactory(schema, config, mode, targetClass) 
        val rowConversion = buildConversion(schema, targetClass)
        var rowIter = iterator
        var queryTargets = null.asInstanceOf[List[ObjectTarget]]
        
        // If update by value requested, use a duplicate iterator to find targets
        if(!updateByValueField.isEmpty()){
          val iters = iterator.duplicate
          rowIter = iters._1
          queryTargets = getQueryTargets(iters._2, rowConversion, targetClass, updateByValueField)
          logDebug(s"Computed ${queryTargets.size} query targets")
        }
        
        while (rowIter.hasNext) {
          val row = rowIter.next()
          
          // Get the target object, this may have been found by value,
          // OID or created as new instance depending on write modes
          val targetObj = 
            if(queryTargets != null){
              // If the query found a target, use it otherwise consult factory
              val queryTarget = queryTargets.get(counter).getInstance
              if(queryTarget != null) queryTarget else instanceFactory(row)
            } else {instanceFactory(row)}
          
          // Write the row data to the target object
          processRow(row, targetObj, rowConversion, appendArrays)
          counter += 1;         
        }
        logDebug(s"Commiting partition of $counter objects during save")
        tx.complete()
      } 
    } 
  }
  
  private def getQueryTargets(
      rowIter: Iterator[Row], 
      rowConversion: Array[ConversionSpecification], 
      targetClass: Class, fieldName: String) : List[ObjectTarget] = {
    // get the field conversion for the update key
    val fieldConversion = rowConversion.find(_.name.equals(fieldName)).getOrElse(
        sys.error(s"updateByValue field (fieldName) does not exist or cannot be converted"))
    val fieldIdx = fieldConversion.colIdx
    val fieldAttribute = targetClass.lookupAttribute(fieldName)
    val targetFinder = new TargetFinder()
    val queryTargets = rowIter.toList.map { row => 
       val fieldValue = fieldConversion.convert(fieldIdx, row)
       val key = new ObjectTargetKeyBuilder(targetClass.getName).add(fieldName, 
          new Variable(fieldValue)).build()
       logDebug(s"Creating query target for ${targetClass.getName} : $fieldName : $fieldValue")
       targetFinder.getObjectTarget(key)
    }
    targetFinder.resolveTargets()
    return queryTargets
  }
  
  private def processRow(row: Row, targetObj: Instance, 
      conversion: Array[ConversionSpecification], appendArrays: Boolean): Unit = {
    
    if(targetObj != null){
      val valueHolder = new Variable()
      conversion.foreach { cs =>  
        
        if(!cs.children.isEmpty){  
          // process embedded row using child conversion
          targetObj.getAttributeValue(cs.attribute, valueHolder)
          val embeddedTarget = valueHolder.instanceValue()
          val embeddedRow = row.getStruct(cs.colIdx)
          processRow(embeddedRow, embeddedTarget, cs.children, true)
          
        } else if(cs.array) {
          // process array row field as a List. If not appending, delete
          // the current elements otherwise add to existing
          targetObj.getAttributeValue(cs.attribute, valueHolder)
          val list = valueHolder.listValue()
          if(!appendArrays) list.clear()
          val arrayRow = Row.fromSeq(row.getSeq(cs.colIdx))
          for(i <- 0 until arrayRow.size){
            val elem = cs.convert(i, arrayRow)
            list.add(new Variable(elem))
          }
          
        } else {
          // Standard field conversion, use converter directly 
          targetObj.getAttributeValue(cs.attribute, valueHolder)
          valueHolder.set(cs.convert(cs.colIdx, row))
        }
      }
    } else {
      logDebug(s"No (null) target object for writing  row : $row")
    }
  }
  
  private def getInstanceFactory(schema: StructType, config: ObjyConfig, 
      mode: SaveMode, targetClass: com.objy.data.Class) : (Row) => Instance = {
    
    val targetByOidColumn = config.get(UpdateByOid)
    val updateByValueColumn = config.get(UpdateByValue)   

    // If updateByValue only create upsert instance in Append mode
    if(!updateByValueColumn.isEmpty()){
      if(mode == SaveMode.Append) (r: Row) => Instance.createPersistent(targetClass)
      else (r: Row) => null.asInstanceOf[Instance]     
    }
    // When targeting OID, only lookup, do not create
    else if(!targetByOidColumn.isEmpty()){
      val oidIdx = schema.fieldIndex(targetByOidColumn)
      (r: Row) => Instance.lookup(new ObjectId(r.getLong(oidIdx)))
    } 
    // Not updating, therefore all instances are created new
    else {
      (r: Row) => Instance.createPersistent(targetClass)
    }   
  }
  
  private def buildConversion(schema: StructType, objyClass: com.objy.data.Class): 
    Array[ConversionSpecification] = {
    
    // for each attribute of the class attempt to map to a schema field
    objyClass.getAttributes.toArray.flatMap(a => {
      val attribute = a.attributeValue
      try{
        val idx = schema.fieldIndex(attribute.getName)
        val field = schema.get(idx)
        field.dataType match {
          case StructType(_) => getEmbeddedConversion(idx, attribute, field)
          case ArrayType(_,_) => getFieldConversion(idx, attribute, field, true)        
          case _ => getFieldConversion(idx, attribute, field, false)
        }
      } catch {
        // no schema field exists for class field
        case iae: IllegalArgumentException => None
      }
    })         
  }

  private def getEmbeddedConversion(idx: Integer, attr: Attribute, field: StructField): 
    Option[ConversionSpecification] = {
    // TODO : correct conversion for embedded's
    None
  }
  
  private def getFieldConversion(idx: Integer, attr: Attribute, 
      field: StructField, array:Boolean): Option[ConversionSpecification] = {
    
      // use the fields datatype unless its an array, then use the element type
      val dataType = if(array) field.dataType.asInstanceOf[ArrayType].elementType else field.dataType
      val attrSpec = if(array) attr.getAttributeValueSpecification.getElementSpecification 
                     else attr.getAttributeValueSpecification
      // see if the registry contains a conversion for the filed/attribute pair
      ULBConversionRegistry.getConversion(dataType, attrSpec, false).map(converter => {
        
        // map the converter function and wrap details in a ConversionSpecification
        ConversionSpecification(idx, field.name, attr, array, Array.empty, converter)
      })           
  }
  
  private case class ConversionSpecification(
      colIdx: Integer,
      name: String,
      attribute: Attribute,
      array: Boolean,
      children: Array[ConversionSpecification],
      convert: (Integer, Row) => Any
  )
}
