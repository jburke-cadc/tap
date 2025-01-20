/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2024.                            (c) 2024.
 *  Government of Canada                 Gouvernement du Canada
 *  National Research Council            Conseil national de recherches
 *  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
 *  All rights reserved                  Tous droits réservés
 *
 *  NRC disclaims any warranties,        Le CNRC dénie toute garantie
 *  expressed, implied, or               énoncée, implicite ou légale,
 *  statutory, of any kind with          de quelque nature que ce
 *  respect to the software,             soit, concernant le logiciel,
 *  including without limitation         y compris sans restriction
 *  any warranty of merchantability      toute garantie de valeur
 *  or fitness for a particular          marchande ou de pertinence
 *  purpose. NRC shall not be            pour un usage particulier.
 *  liable in any event for any          Le CNRC ne pourra en aucun cas
 *  damages, whether direct or           être tenu responsable de tout
 *  indirect, special or general,        dommage, direct ou indirect,
 *  consequential or incidental,         particulier ou général,
 *  arising from the use of the          accessoire ou fortuit, résultant
 *  software.  Neither the name          de l'utilisation du logiciel. Ni
 *  of the National Research             le nom du Conseil National de
 *  Council of Canada nor the            Recherches du Canada ni les noms
 *  names of its contributors may        de ses  participants ne peuvent
 *  be used to endorse or promote        être utilisés pour approuver ou
 *  products derived from this           promouvoir les produits dérivés
 *  software without specific prior      de ce logiciel sans autorisation
 *  written permission.                  préalable et particulière
 *                                       par écrit.
 *
 *  This file is part of the             Ce fichier fait partie du projet
 *  OpenCADC project.                    OpenCADC.
 *
 *  OpenCADC is free software:           OpenCADC est un logiciel libre ;
 *  you can redistribute it and/or       vous pouvez le redistribuer ou le
 *  modify it under the terms of         modifier suivant les termes de
 *  the GNU Affero General Public        la “GNU Affero General Public
 *  License as published by the          License” telle que publiée
 *  Free Software Foundation,            par la Free Software Foundation
 *  either version 3 of the              : soit la version 3 de cette
 *  License, or (at your option)         licence, soit (à votre gré)
 *  any later version.                   toute version ultérieure.
 *
 *  OpenCADC is distributed in the       OpenCADC est distribué
 *  hope that it will be useful,         dans l’espoir qu’il vous
 *  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
 *  without even the implied             GARANTIE : sans même la garantie
 *  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
 *  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
 *  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
 *  General Public License for           Générale Publique GNU Affero
 *  more details.                        pour plus de détails.
 *
 *  You should have received             Vous devriez avoir reçu une
 *  a copy of the GNU Affero             copie de la Licence Générale
 *  General Public License along         Publique GNU Affero avec
 *  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
 *  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
 *                                       <http://www.gnu.org/licenses/>.
 *
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.youcat;

import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.dali.tables.votable.VOTableField;
import ca.nrc.cadc.dali.tables.votable.VOTableGroup;
import ca.nrc.cadc.dali.tables.votable.VOTableInfo;
import ca.nrc.cadc.dali.tables.votable.VOTableParam;
import ca.nrc.cadc.dali.tables.votable.VOTableReader;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableTable;
import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpDelete;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayOutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import javax.security.auth.Subject;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

public class DescriptorsTableTest extends AbstractTablesTest {
    static final Logger log = Logger.getLogger(DescriptorsTableTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.youcat", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.tap", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.reg", Level.DEBUG);
    }

    static final String VOTABLE_MIME_TYPE = "application/x-votable+xml";
    static final String DESCRIPTORS_TABLE_NAME = "tap_schema.ServiceDescriptors";

    final DataSource dataSource;

    public DescriptorsTableTest() {
        try {
            DBConfig conf = new DBConfig();
            ConnectionConfig cc = conf.getConnectionConfig("YOUCAT_TEST", "cadctest");
            this.dataSource = DBUtil.getDataSource(cc);
            log.debug("configured data source: " + cc.getServer() + "," + cc.getDatabase() + "," + cc.getDriver() + "," + cc.getURL());
        } catch (Throwable t) {
            throw new RuntimeException("TEST SETUP FAILED", t);
        }
    }

    @Test
    public void testDescriptor() {
        log.info("testDescriptor()");
        try {
            // delete existing content
            try {
                String delete = "DELETE FROM " + DESCRIPTORS_TABLE_NAME;
                JdbcTemplate jdbc = new JdbcTemplate(dataSource);
                jdbc.execute(delete);
                log.info("successfully deleted from: " + DESCRIPTORS_TABLE_NAME);
            } catch (Exception ignore) {
                log.debug("cleanup-before-test failed for " + DESCRIPTORS_TABLE_NAME);
            }

            String descriptorName = "";

            // PUT a new service descriptor
            URL testURL = new URL(String.format("%s/%s", descriptorsURL, descriptorName));
            log.debug("test descriptor URL: " + testURL);
            VOTableDocument expected = getServiceDescriptor();
            VOTableWriter writer = new VOTableWriter();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            writer.write(expected, out);
            FileContent fileContent = new FileContent(out.toByteArray(), VOTABLE_MIME_TYPE);
            HttpUpload put = new HttpUpload(fileContent, testURL);
            Subject.doAs(admin, new RunnableAction(put));
            Assert.assertNull(put.getThrowable());
            Assert.assertEquals(201, put.getResponseCode());
            log.debug("created service descriptor");

            // GET the service descriptor
            out = new ByteArrayOutputStream();
            HttpGet get = new HttpGet(testURL, out);
            Subject.doAs(admin, new RunnableAction(get));
            Assert.assertNull(get.getThrowable());
            Assert.assertEquals(200, get.getResponseCode());
            log.debug("got service descriptor");
            VOTableReader reader = new VOTableReader();
            VOTableDocument actual = reader.read(out.toString(StandardCharsets.UTF_8));
            compare(expected, actual);

            // UPDATE the service descriptor
            expected.getResources().get(0).getInfos().get(0).content = "new content";
            out = new ByteArrayOutputStream();
            writer.write(expected, out);
            fileContent = new FileContent(out.toByteArray(), VOTABLE_MIME_TYPE);
            HttpPost post = new HttpPost(testURL, fileContent, true);
            Subject.doAs(admin, new RunnableAction(post));
            Assert.assertNull(post.getThrowable());
            Assert.assertEquals(200, post.getResponseCode());
            log.debug("updated service descriptor");

            // GET the updated service descriptor
            out = new ByteArrayOutputStream();
            get = new HttpGet(testURL, out);
            Subject.doAs(admin, new RunnableAction(get));
            Assert.assertNull(get.getThrowable());
            Assert.assertEquals(200, get.getResponseCode());
            log.debug("got updated service descriptor");
            reader = new VOTableReader();
            actual = reader.read(out.toString(StandardCharsets.UTF_8));
            compare(expected, actual);

            // DELETE the service descriptor
            HttpDelete delete = new HttpDelete(testURL, true);
            Subject.doAs(admin, new RunnableAction(delete));
            Assert.assertNull(delete.getThrowable());
            Assert.assertEquals(200, delete.getResponseCode());
            out = new ByteArrayOutputStream();
            log.debug("deleted service descriptor");

            // GET the deleted service descriptor
            out = new ByteArrayOutputStream();
            get = new HttpGet(testURL, out);
            Subject.doAs(admin, new RunnableAction(get));
            Assert.assertNotNull(get.getThrowable());
            Assert.assertEquals(404, get.getResponseCode());
            log.debug("deleted service descriptor not found");

        } catch (Exception t) {
            log.error("unexpected", t);
            Assert.fail("unexpected: " + t.getMessage());
        } finally {
            log.info("testDescriptor done");
        }
    }

    VOTableDocument getServiceDescriptor() {
        VOTableDocument votable = new VOTableDocument();

        VOTableInfo info = new VOTableInfo("name", "my test service");
        info.id = "testID";
        info.content = "content";
        votable.getInfos().add(info);

        VOTableResource resource = new VOTableResource("meta");
        votable.getResources().add(resource);

        VOTableGroup group = new VOTableGroup("inputParams");
        resource.getGroups().add(group);

        VOTableParam param = new VOTableParam("ID", "char", "*", "");
        param.ref = "testID";
        param.xtype = "uri";
        group.getParams().add(param);

        return votable;
    }

    void compare(VOTableDocument expected, VOTableDocument actual) {

        Assert.assertEquals(expected.getInfos().size(), actual.getInfos().size());
        VOTableInfo expectedInfo = expected.getInfos().get(0);
        VOTableInfo actualInfo = actual.getInfos().get(0);
        Assert.assertEquals(expectedInfo.getName(), actualInfo.getName());
        Assert.assertEquals(expectedInfo.getValue(), actualInfo.getValue());
        Assert.assertEquals(expectedInfo.id, actualInfo.id);
        Assert.assertEquals(expectedInfo.content, actualInfo.content);

        Assert.assertEquals(expected.getResources().size(), actual.getResources().size());
        VOTableResource expectedResource = expected.getResources().get(0);
        VOTableResource actualResource = actual.getResources().get(0);
        Assert.assertEquals(expectedResource.getType(), actualResource.getType());

        Assert.assertEquals(expectedResource.getGroups().size(), actualResource.getGroups().size());
        VOTableGroup expectedGroup = expectedResource.getGroups().get(0);
        VOTableGroup actualGroup = actualResource.getGroups().get(0);
        Assert.assertEquals(expectedGroup.getName(), actualGroup.getName());

        Assert.assertEquals(expectedGroup.getParams().size(), actualGroup.getParams().size());
        VOTableParam expectedParam = expectedGroup.getParams().get(0);
        VOTableParam actualParam = actualGroup.getParams().get(0);
        Assert.assertEquals(expectedParam.getName(), actualParam.getName());
        Assert.assertEquals(expectedParam.getDatatype(), actualParam.getDatatype());
        Assert.assertEquals(expectedParam.getArraysize(), actualParam.getArraysize());
        Assert.assertEquals(expectedParam.getValue(), actualParam.getValue());
        Assert.assertEquals(expectedParam.ref, actualParam.ref);
        Assert.assertEquals(expectedParam.xtype, actualParam.xtype);
    }

}
