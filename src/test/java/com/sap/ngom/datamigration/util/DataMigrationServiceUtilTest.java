package com.sap.ngom.datamigration.util;

import com.sap.ngom.datamigration.exception.DataMigrationProcessException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.List;

public class DataMigrationServiceUtilTest {

    @InjectMocks
    private DataMigrationServiceUtil dataMigrationServiceUtil;

    String[] sourceTableNames = {"bp","address"};
    List<String> sourceTableNamesList = Arrays.asList(sourceTableNames);

    @Before
    public void setup(){
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetSourceTableNameWithoutNameSpace(){
        ReflectionTestUtils.setField(dataMigrationServiceUtil, "targetTableNameSpace", "");
        ReflectionTestUtils.setField(dataMigrationServiceUtil, "sourceTableNames", sourceTableNames);
        ReflectionTestUtils.setField(dataMigrationServiceUtil, "sourceTableNamesList", sourceTableNamesList);
        Assert.assertEquals(2,dataMigrationServiceUtil.getSourceTableNames().size());
        Assert.assertSame("bp",dataMigrationServiceUtil.getSourceTableNames().get(0));
        Assert.assertSame("address",dataMigrationServiceUtil.getSourceTableNames().get(1));
    }

    @Test
    public void testGetSourceTableNameWithNameSpace(){
        String nameSpace = "com.sap.ngom.test";
        ReflectionTestUtils.setField(dataMigrationServiceUtil, "targetTableNameSpace", nameSpace);
        ReflectionTestUtils.setField(dataMigrationServiceUtil, "sourceTableNames", sourceTableNames);
        ReflectionTestUtils.setField(dataMigrationServiceUtil, "sourceTableNamesList", sourceTableNamesList);
        Assert.assertEquals(2,dataMigrationServiceUtil.getSourceTableNames().size());
        Assert.assertEquals("com.sap.ngom.test.bp",nameSpace + "." + dataMigrationServiceUtil.getSourceTableNames().get(0));
        Assert.assertEquals("com.sap.ngom.test.address",nameSpace + "." + dataMigrationServiceUtil.getSourceTableNames().get(1));
    }

    @Test
    public void testGetTargetTableNameWithNameSpace(){
        String nameSpace = "com.sap.ngom.test";
        ReflectionTestUtils.setField(dataMigrationServiceUtil, "targetTableNameSpace", nameSpace);
        ReflectionTestUtils.setField(dataMigrationServiceUtil, "sourceTableNames", sourceTableNames);
        ReflectionTestUtils.setField(dataMigrationServiceUtil, "sourceTableNamesList", sourceTableNamesList);
        Assert.assertEquals("com.sap.ngom.test.bp",dataMigrationServiceUtil.getTargetTableName("bp"));
    }

    @Test
    public void testGetTargetTableNameWithoutNameSpace(){
        String nameSpace = "";
        ReflectionTestUtils.setField(dataMigrationServiceUtil, "targetTableNameSpace", nameSpace);
        ReflectionTestUtils.setField(dataMigrationServiceUtil, "sourceTableNames", sourceTableNames);
        ReflectionTestUtils.setField(dataMigrationServiceUtil, "sourceTableNamesList", sourceTableNamesList);
        Assert.assertEquals("bp",dataMigrationServiceUtil.getTargetTableName("bp"));
    }

    @Test(expected = DataMigrationProcessException.class)
    public void testGetTargetTableNameWithInvalidSourceTable(){
        String nameSpace = "";
        ReflectionTestUtils.setField(dataMigrationServiceUtil, "targetTableNameSpace", nameSpace);
        ReflectionTestUtils.setField(dataMigrationServiceUtil, "sourceTableNames", sourceTableNames);
        ReflectionTestUtils.setField(dataMigrationServiceUtil, "sourceTableNamesList", sourceTableNamesList);
        Assert.assertEquals("bp",dataMigrationServiceUtil.getTargetTableName("fake"));
    }

    @Test
    public void testGetTargetTableNameWithNameSpaceNull(){
        String nameSpace = null;
        ReflectionTestUtils.setField(dataMigrationServiceUtil, "targetTableNameSpace", nameSpace);
        ReflectionTestUtils.setField(dataMigrationServiceUtil, "sourceTableNames", sourceTableNames);
        ReflectionTestUtils.setField(dataMigrationServiceUtil, "sourceTableNamesList", sourceTableNamesList);
        Assert.assertEquals("bp",dataMigrationServiceUtil.getTargetTableName("bp"));
    }
}
