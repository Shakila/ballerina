/*
 * Copyright (c) 2017, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 * <p>
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.ballerinalang.test.expressions.conversion;

import com.fasterxml.jackson.databind.JsonNode;

import org.ballerinalang.model.values.BBoolean;
import org.ballerinalang.model.values.BFloat;
import org.ballerinalang.model.values.BIntArray;
import org.ballerinalang.model.values.BInteger;
import org.ballerinalang.model.values.BJSON;
import org.ballerinalang.model.values.BMap;
import org.ballerinalang.model.values.BRefValueArray;
import org.ballerinalang.model.values.BString;
import org.ballerinalang.model.values.BStringArray;
import org.ballerinalang.model.values.BStruct;
import org.ballerinalang.model.values.BValue;
import org.ballerinalang.test.utils.BTestUtils;
import org.ballerinalang.test.utils.CompileResult;
import org.ballerinalang.util.exceptions.BLangRuntimeException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test Cases for type conversion.
 */
@Test
public class NativeConversionTest {

    private CompileResult compileResult;
    private CompileResult negativeResult;

    @BeforeClass
    public void setup() {
        compileResult = BTestUtils.compile("test-src/expressions/conversion/native-conversion.bal");
        negativeResult = BTestUtils.compile("test-src/expressions/conversion/native-conversion-negative.bal");
    }

    @Test
    public void testStructToMap() {
        BValue[] returns = BTestUtils.invoke(compileResult, "testStructToMap");
        Assert.assertTrue(returns[0] instanceof BMap<?, ?>);
        BMap<String, ?> map = (BMap<String, ?>) returns[0];

        BValue name = map.get("name");
        Assert.assertTrue(name instanceof BString);
        Assert.assertEquals(name.stringValue(), "Child");

        BValue age = map.get("age");
        Assert.assertTrue(age instanceof BInteger);
        Assert.assertEquals(((BInteger) age).intValue(), 25);

        BValue parent = map.get("parent");
        Assert.assertTrue(parent instanceof BStruct);
        BStruct parentStruct = (BStruct) parent;
        Assert.assertEquals(parentStruct.getStringField(0), "Parent");
        Assert.assertEquals(parentStruct.getIntField(0), 50);

        BValue address = map.get("address");
        Assert.assertTrue(address instanceof BMap<?, ?>);
        BMap<String, ?> addressMap = (BMap<String, ?>) address;
        Assert.assertEquals(addressMap.get("city").stringValue(), "Colombo");
        Assert.assertEquals(addressMap.get("country").stringValue(), "SriLanka");

        BValue info = map.get("info");
        Assert.assertTrue(info instanceof BJSON);
        Assert.assertEquals(info.stringValue(), "{\"status\":\"single\"}");

        BValue marks = map.get("marks");
        Assert.assertTrue(marks instanceof BIntArray);
        BIntArray marksArray = (BIntArray) marks;
        Assert.assertEquals(marksArray.get(0), 67);
        Assert.assertEquals(marksArray.get(1), 38);
        Assert.assertEquals(marksArray.get(2), 91);
    }

    @Test
    public void testMapToStruct() {
        BValue[] returns = BTestUtils.invoke(compileResult, "testMapToStruct");
        Assert.assertTrue(returns[0] instanceof BStruct);
        BStruct struct = (BStruct) returns[0];

        String name = struct.getStringField(0);
        Assert.assertEquals(name, "Child");

        long age = struct.getIntField(0);
        Assert.assertEquals(age, 25);

        BValue parent = struct.getRefField(0);
        Assert.assertTrue(parent instanceof BStruct);
        BStruct parentStruct = (BStruct) parent;
        Assert.assertEquals(parentStruct.getStringField(0), "Parent");
        Assert.assertEquals(parentStruct.getIntField(0), 50);
        Assert.assertEquals(parentStruct.getRefField(0), null);
        Assert.assertEquals(parentStruct.getRefField(1), null);
        Assert.assertEquals(parentStruct.getRefField(2), null);
        Assert.assertEquals(parentStruct.getRefField(3), null);

        BValue info = struct.getRefField(1);
        Assert.assertTrue(info instanceof BJSON);
        Assert.assertEquals(info.stringValue(), "{\"status\":\"single\"}");

        BValue address = struct.getRefField(2);
        Assert.assertTrue(address instanceof BMap<?, ?>);
        BMap<String, ?> addressMap = (BMap<String, ?>) address;
        Assert.assertEquals(addressMap.get("city").stringValue(), "Colombo");
        Assert.assertEquals(addressMap.get("country").stringValue(), "SriLanka");

        BValue marks = struct.getRefField(3);
        Assert.assertTrue(marks instanceof BIntArray);
        BIntArray marksArray = (BIntArray) marks;
        Assert.assertEquals(marksArray.get(0), 87);
        Assert.assertEquals(marksArray.get(1), 94);
        Assert.assertEquals(marksArray.get(2), 72);
    }

    @Test
    public void testJsonToStruct() {
        BValue[] returns = BTestUtils.invoke(compileResult, "testJsonToStruct");
        Assert.assertTrue(returns[0] instanceof BStruct);
        BStruct struct = (BStruct) returns[0];

        String name = struct.getStringField(0);
        Assert.assertEquals(name, "Child");

        long age = struct.getIntField(0);
        Assert.assertEquals(age, 25);

        BValue parent = struct.getRefField(0);
        Assert.assertTrue(parent instanceof BStruct);
        BStruct parentStruct = (BStruct) parent;
        Assert.assertEquals(parentStruct.getStringField(0), "Parent");
        Assert.assertEquals(parentStruct.getIntField(0), 50);
        Assert.assertEquals(parentStruct.getRefField(0), null);
        Assert.assertEquals(parentStruct.getRefField(1), null);
        Assert.assertEquals(parentStruct.getRefField(2), null);
        Assert.assertEquals(parentStruct.getRefField(3), null);

        BValue info = struct.getRefField(1);
        Assert.assertTrue(info instanceof BJSON);
        Assert.assertEquals(info.stringValue(), "{\"status\":\"single\"}");

        BValue address = struct.getRefField(2);
        Assert.assertTrue(address instanceof BMap<?, ?>);
        BMap<String, ?> addressMap = (BMap<String, ?>) address;
        Assert.assertEquals(addressMap.get("city").stringValue(), "Colombo");
        Assert.assertEquals(addressMap.get("country").stringValue(), "SriLanka");

        BValue marks = struct.getRefField(3);
        Assert.assertTrue(marks instanceof BIntArray);
        BIntArray marksArray = (BIntArray) marks;
        Assert.assertEquals(marksArray.size(), 2);
        Assert.assertEquals(marksArray.get(0), 56);
        Assert.assertEquals(marksArray.get(1), 79);
    }

    @Test
    public void testStructToJson() {
        BValue[] returns = BTestUtils.invoke(compileResult, "testStructToJson");
        Assert.assertTrue(returns[0] instanceof BJSON);
        JsonNode child = ((BJSON) returns[0]).value();
        Assert.assertEquals(child.get("name").textValue(), "Child");
        Assert.assertEquals(child.get("age").intValue(), 25);

        JsonNode parent = child.get("parent");
        Assert.assertTrue(parent.isObject());
        Assert.assertEquals(parent.get("name").textValue(), "Parent");
        Assert.assertEquals(parent.get("age").intValue(), 50);
        Assert.assertTrue(parent.get("parent").isNull());
        Assert.assertTrue(parent.get("info").isNull());
        Assert.assertTrue(parent.get("address").isNull());
        Assert.assertTrue(parent.get("marks").isNull());

        JsonNode info = child.get("info");
        Assert.assertTrue(info.isObject());
        Assert.assertEquals(info.get("status").textValue(), "single");

        JsonNode address = child.get("address");
        Assert.assertTrue(info.isObject());
        Assert.assertEquals(address.get("country").textValue(), "SriLanka");
        Assert.assertEquals(address.get("city").textValue(), "Colombo");

        JsonNode marks = child.get("marks");
        Assert.assertTrue(marks.isArray());
        Assert.assertEquals(marks.size(), 3);
        Assert.assertEquals(marks.get(0).intValue(), 87);
        Assert.assertEquals(marks.get(1).intValue(), 94);
        Assert.assertEquals(marks.get(2).intValue(), 72);
    }

    @Test(description = "Test converting a struct to a struct")
    public void testStructToStruct() {
        BTestUtils.validateError(negativeResult, 0,
                "incompatible types: 'Person' cannot be convert to 'Student', use cast expression", 26, 17);
    }

    @Test(description = "Test converting a map to json")
    public void testMapToJsonConversionError() {
        BTestUtils.validateError(negativeResult, 1, "incompatible types: 'map' cannot be convert to 'json'", 36, 15);
    }

    @Test(expectedExceptions = {BLangRuntimeException.class},
          expectedExceptionsMessageRegExp = ".*cannot convert 'map' to type 'Person: error while mapping 'info': " +
                  "incompatible types: expected 'json', found 'map'.*")
    public void testIncompatibleMapToStruct() {
        BTestUtils.invoke(compileResult, "testIncompatibleMapToStruct");
    }

    @Test(description = "Test converting a map with missing field to a struct")
    public void testMapWithMissingFieldsToStruct() {
        BValue[] returns = BTestUtils.invoke(compileResult, "testMapWithMissingFieldsToStruct");
        Assert.assertTrue(returns[0] instanceof BStruct);
        BStruct personStruct = (BStruct) returns[0];

        Assert.assertEquals(personStruct.stringValue(), "{name:\"Child\", age:25, parent:null, info:null, " +
                "address:{\"city\":\"Colombo\", \"country\":\"SriLanka\"}, marks:[87, 94, 72], a:null, score:0.0, " +
                "alive:false, children:null}");
    }

    @Test(description = "Test converting a map with incompatible inner array to a struct",
          expectedExceptions = {BLangRuntimeException.class},
          expectedExceptionsMessageRegExp = ".*cannot convert 'map' to type 'Person: error while mapping 'marks': " +
                  "incompatible types: expected 'int\\[\\]', found 'float\\[\\]'.*")
    public void testMapWithIncompatibleArrayToStruct() {
        BTestUtils.invoke(compileResult, "testMapWithIncompatibleArrayToStruct");
    }

    // TODO With the latest changes introduced to BLangVM, this test does not return an error
    // TODO Because Student struct is compatible with Person struct.
//    @Test(description = "Test converting a map with incompatible inner struct to a struct",
//            expectedExceptions = { BLangRuntimeException.class },
//            expectedExceptionsMessageRegExp = ".*cannot cast 'map' to type 'Employee:
// error while mapping 'partner':" +
//            " incompatible types: expected 'Person', found 'Student'.*")
    public void testMapWithIncompatibleStructToStruct() {
        BTestUtils.invoke(compileResult, "testMapWithIncompatibleStructToStruct");
    }

    @Test(description = "Test converting a incompatible JSON to a struct",
          expectedExceptions = {BLangRuntimeException.class},
          expectedExceptionsMessageRegExp = ".*cannot convert 'json' to type 'Person': error while " +
                  "mapping 'age': incompatible types: expected 'int', found 'string' in json.*")
    public void testIncompatibleJsonToStruct() {
        BTestUtils.invoke(compileResult, "testIncompatibleJsonToStruct");
    }

    //    @Test
    public void testJsonToStructWithMissingFields() {
        BValue[] returns = BTestUtils.invoke(compileResult, "testJsonToStructWithMissingFields");
        Assert.assertEquals(returns[0].stringValue(), "{name:\"Child\", age:25, parent:null, info:" +
                "{\"status\":\"single\"}, address:{\"city\":\"Colombo\", \"country\":\"SriLanka\"}, " +
                "marks:[87, 94, 72], a:null, score:0.0, alive:false, children:null}");
    }

    @Test(description = "Test converting a JSON with incompatible inner map to a struct",
          expectedExceptions = {BLangRuntimeException.class},
          expectedExceptionsMessageRegExp = ".*cannot convert 'json' to type 'Person': error while mapping " +
                  "'address': incompatible types: expected 'json-object', found 'string'.*")
    public void testJsonWithIncompatibleMapToStruct() {
        BTestUtils.invoke(compileResult, "testJsonWithIncompatibleMapToStruct");
    }

    @Test(description = "Test converting a JSON with incompatible inner struct to a struct",
          expectedExceptions = {BLangRuntimeException.class},
          expectedExceptionsMessageRegExp = ".*cannot convert 'json' to type 'Person': error while " +
                  "mapping 'parent': incompatible types: expected 'json-object', found 'string'.*")
    public void testJsonWithIncompatibleStructToStruct() {
        BTestUtils.invoke(compileResult, "testJsonWithIncompatibleStructToStruct");
    }

    @Test(description = "Test converting a JSON array to a struct",
          expectedExceptions = {BLangRuntimeException.class},
          expectedExceptionsMessageRegExp = ".*cannot convert 'json' to type 'Person': incompatible " +
                  "types: expected 'json-object', found 'json-array'.*")
    public void testJsonArrayToStruct() {
        BTestUtils.invoke(compileResult, "testJsonArrayToStruct");
    }

    @Test(description = "Test converting a JSON with incompatible inner type to a struct",
          expectedExceptions = {BLangRuntimeException.class},
          expectedExceptionsMessageRegExp = ".*cannot convert 'json' to type 'Person': error while mapping 'age': " +
                  "incompatible types: expected 'int', found 'float' in json.*")
    public void testJsonWithIncompatibleTypeToStruct() {
        BTestUtils.invoke(compileResult, "testJsonWithIncompatibleTypeToStruct");
    }

    @Test(description = "Test converting a struct with map of blob to a JSON",
            expectedExceptions = { BLangRuntimeException.class },
            expectedExceptionsMessageRegExp = ".*cannot convert 'Info' to type 'json': error while mapping 'bar': " +
                    "incompatible types: expected 'json', found 'blob'.*")
    public void testStructWithIncompatibleTypeMapToJson() {
        BTestUtils.invoke(compileResult, "testStructWithIncompatibleTypeMapToJson");
    }

    @Test(description = "Test converting a struct with map of blob to a JSON")
    public void testStructWithIncompatibleTypeToJson() {
        BTestUtils.validateError(negativeResult, 2, "incompatible types: 'Info' cannot be convert to 'json'", 49, 14);
    }

    @Test(description = "Test converting a JSON array to any array")
    public void testJsonToAnyArray() {
        BValue[] returns = BTestUtils.invoke(compileResult, "testJsonToAnyArray");
        Assert.assertTrue(returns[0] instanceof BStruct);
        BStruct anyArrayStruct = (BStruct) returns[0];
        BRefValueArray array = (BRefValueArray) anyArrayStruct.getRefField(0);

        Assert.assertEquals(((BInteger) array.get(0)).intValue(), 4);
        Assert.assertEquals(array.get(1).stringValue(), "Supun");
        Assert.assertEquals(((BFloat) array.get(2)).floatValue(), 5.36);
        Assert.assertEquals(((BBoolean) array.get(3)).booleanValue(), true);
        Assert.assertEquals(((BJSON) array.get(4)).stringValue(), "{\"lname\":\"Setunga\"}");
        Assert.assertEquals(((BJSON) array.get(5)).stringValue(), "[4,3,7]");
        Assert.assertEquals(array.get(6), null);
    }

    @Test(description = "Test converting a JSON array to int array")
    public void testJsonToIntArray() {
        BValue[] returns = BTestUtils.invoke(compileResult, "testJsonToIntArray");
        Assert.assertTrue(returns[0] instanceof BStruct);
        BStruct anyArrayStruct = (BStruct) returns[0];
        BIntArray array = (BIntArray) anyArrayStruct.getRefField(0);

        Assert.assertEquals(array.getType().toString(), "int[]");
        Assert.assertEquals(array.get(0), 4);
        Assert.assertEquals(array.get(1), 3);
        Assert.assertEquals(array.get(2), 9);
    }

    @Test(description = "Test converting a JSON string array to string array")
    public void testJsonToStringArray() {
        BValue[] returns = BTestUtils.invoke(compileResult, "testJsonToStringArray");
        Assert.assertTrue(returns[0] instanceof BStruct);
        BStruct anyArrayStruct = (BStruct) returns[0];
        BStringArray array = (BStringArray) anyArrayStruct.getRefField(0);

        Assert.assertEquals(array.getType().toString(), "string[]");
        Assert.assertEquals(array.get(0), "a");
        Assert.assertEquals(array.get(1), "b");
        Assert.assertEquals(array.get(2), "c");
    }

    @Test(description = "Test converting a JSON integer array to string array")
    public void testJsonIntArrayToStringArray() {
        BValue[] returns = BTestUtils.invoke(compileResult, "testJsonIntArrayToStringArray");
        Assert.assertTrue(returns[0] instanceof BStruct);
        BStruct anyArrayStruct = (BStruct) returns[0];
        BStringArray array = (BStringArray) anyArrayStruct.getRefField(0);

        Assert.assertEquals(array.getType().toString(), "string[]");
        Assert.assertEquals(array.get(0), "4");
        Assert.assertEquals(array.get(1), "3");
        Assert.assertEquals(array.get(2), "9");
    }

    @Test(description = "Test converting a JSON array to xml array",
          expectedExceptions = {BLangRuntimeException.class},
          expectedExceptionsMessageRegExp = ".*cannot convert 'json' to type 'XmlArray': error while mapping 'a': " +
                  "incompatible types: expected 'xml', found 'string'.*")
    public void testJsonToXmlArray() {
        BTestUtils.invoke(compileResult, "testJsonToXmlArray");
    }

    @Test(description = "Test converting a JSON integer array to string array")
    public void testNullJsonToArray() {
        BValue[] returns = BTestUtils.invoke(compileResult, "testNullJsonToArray");
        Assert.assertEquals(returns[0], null);
    }

    @Test(description = "Test converting a JSON null to string array")
    public void testNullJsonArrayToArray() {
        BValue[] returns = BTestUtils.invoke(compileResult, "testNullJsonArrayToArray");
        Assert.assertTrue(returns[0] instanceof BStruct);
        BStruct anyArrayStruct = (BStruct) returns[0];
        BStringArray array = (BStringArray) anyArrayStruct.getRefField(0);

        Assert.assertEquals(array, null);
    }

    @Test(description = "Test converting a JSON string to string array",
          expectedExceptions = {BLangRuntimeException.class},
          expectedExceptionsMessageRegExp = ".*cannot convert 'json' to type 'StringArray': error while " +
                  "mapping 'a': incompatible types: expected 'json-array', found 'string'.*")
    public void testNonArrayJsonToArray() {
        BTestUtils.invoke(compileResult, "testNonArrayJsonToArray");
    }

    // Todo - Fix casting issue
    @Test(description = "Test converting a null JSON to struct")
    public void testNullJsonToStruct() {
        BValue[] returns = BTestUtils.invoke(compileResult, "testNullJsonToStruct");
        Assert.assertEquals(returns[0], null);
    }

    // Todo - Fix casting issue
    @Test(description = "Test converting a null map to Struct")
    public void testNullMapToStruct() {
        BValue[] returns = BTestUtils.invoke(compileResult, "testNullMapToStruct");
        Assert.assertEquals(returns[0], null);
    }

    // Todo - Fix casting issue
    @Test(description = "Test converting a null Struct to json")
    public void testNullStructToJson() {
        BValue[] returns = BTestUtils.invoke(compileResult, "testNullStructToJson");
        Assert.assertEquals(returns[0], null);
    }

    @Test(description = "Test converting a null Struct to map")
    public void testNullStructToMap() {
        BValue[] returns = BTestUtils.invoke(compileResult, "testNullStructToMap");
        Assert.assertEquals(returns[0], null);
    }

    // transform with errors

    @Test
    public void testIncompatibleJsonToStructWithErrors() {
        BValue[] returns = BTestUtils.invoke(compileResult, "testIncompatibleJsonToStructWithErrors",
                                             new BValue[]{});

        // check whether struct is null
        Assert.assertNull(returns[0]);

        // check the error
        Assert.assertTrue(returns[1] instanceof BStruct);
        BStruct error = (BStruct) returns[1];
        String errorMsg = error.getStringField(0);
        Assert.assertEquals(errorMsg, "cannot convert 'json' to type 'Person': error while mapping" +
                " 'parent': incompatible types: expected 'json-object', found 'string'");
    }

    // Todo - Fix casting issue
    @Test
    public void testStructToMapWithRefTypeArray() {
        BValue[] returns = BTestUtils.invoke(compileResult, "testStructToMapWithRefTypeArray");
        Assert.assertTrue(returns[0] instanceof BMap<?, ?>);
        BMap<String, ?> map = (BMap<String, ?>) returns[0];

        BValue name = map.get("title");
        Assert.assertTrue(name instanceof BString);
        Assert.assertEquals(name.stringValue(), "The Revenant");

        BValue age = map.get("year");
        Assert.assertTrue(age instanceof BInteger);
        Assert.assertEquals(((BInteger) age).intValue(), 2015);

        BValue marks = map.get("genre");
        Assert.assertTrue(marks instanceof BStringArray);
        BStringArray genreArray = (BStringArray) marks;
        Assert.assertEquals(genreArray.get(0), "Adventure");
        Assert.assertEquals(genreArray.get(1), "Drama");
        Assert.assertEquals(genreArray.get(2), "Thriller");

        BValue actors = map.get("actors");
        Assert.assertTrue(actors instanceof BRefValueArray);
        BRefValueArray actorsArray = (BRefValueArray) actors;
        Assert.assertTrue(actorsArray.get(0) instanceof BStruct);
        Assert.assertEquals(actorsArray.get(0).stringValue(), "{fname:\"Leonardo\", lname:\"DiCaprio\", age:35}");
    }

    @Test
    public void testStructWithStringArrayToJSON() {
        BValue[] returns = BTestUtils.invoke(compileResult, "testStructWithStringArrayToJSON");
        Assert.assertTrue(returns[0] instanceof BJSON);
        Assert.assertEquals(returns[0].stringValue(), "{\"names\":[\"John\",\"Doe\"]}");
    }

    @Test
    public void testEmptyJSONtoStructWithDefaults() {
        BValue[] returns = BTestUtils.invoke(compileResult, "testEmptyJSONtoStructWithDefaults");
        Assert.assertTrue(returns[0] instanceof BStruct);
        Assert.assertEquals(returns[0].stringValue(), "{s:\"string value\", a:45, f:5.3, b:true, j:null, blb:null}");
    }

    @Test
    public void testEmptyJSONtoStructWithoutDefaults() {
        BValue[] returns = BTestUtils.invoke(compileResult, "testEmptyJSONtoStructWithoutDefaults");
        Assert.assertTrue(returns[0] instanceof BStruct);
        Assert.assertEquals(returns[0].stringValue(), "{s:\"\", a:0, f:0.0, b:false, j:null, blb:null}");
    }

    @Test
    public void testEmptyMaptoStructWithDefaults() {
        BValue[] returns = BTestUtils.invoke(compileResult, "testEmptyMaptoStructWithDefaults");
        Assert.assertTrue(returns[0] instanceof BStruct);
        Assert.assertEquals(returns[0].stringValue(), "{s:\"string value\", a:45, f:5.3, b:true, j:null, blb:null}");
    }

    @Test
    public void testEmptyMaptoStructWithoutDefaults() {
        BValue[] returns = BTestUtils.invoke(compileResult, "testEmptyMaptoStructWithoutDefaults");
        Assert.assertTrue(returns[0] instanceof BStruct);
        Assert.assertEquals(returns[0].stringValue(), "{s:\"\", a:0, f:0.0, b:false, j:null, blb:null}");
    }
}
