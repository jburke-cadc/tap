/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2025.                            (c) 2025.
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

package org.opencadc.youcat.descriptors;

import ca.nrc.cadc.dali.tables.votable.VOTableDocument;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.db.DatabaseTransactionManager;
import ca.nrc.cadc.db.version.KeyValue;
import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.util.StringUtil;
import java.net.HttpURLConnection;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.datalink.ServiceDescriptorTemplate;

public class PutAction extends DescriptorAction {
    private static final Logger log = Logger.getLogger(PutAction.class);

    public PutAction() {
        super();
    }

    @Override
    public void doAction() throws Exception {

        // the path is the descriptor name
        String name = syncInput.getPath();
        if (!StringUtil.hasText(name)) {
            throw new IllegalArgumentException("Expected path of descriptor name, found: " + name);
        }
        if (name.split("/").length != 1) {
            throw new IllegalArgumentException("Expected single path component, found: " + name);
        }
        log.debug("name: " + name);

        // check for an existing descriptor
        KeyValue keyValue = keyValueDAO.get(name);
        if (keyValue != null) {
            throw new ResourceAlreadyExistsException("Found existing descriptor: " + name);
        }

        // Create a ServiceDescriptorTemplate to validate the VOTable
        VOTableDocument document = getVOTableDocument();
        String votable = document2String(document);
        ServiceDescriptorTemplate serviceDescriptor = new ServiceDescriptorTemplate(name, votable);

        // create the descriptor
        DataSource dataSource = DBUtil.findJNDIDataSource("jdbc/tapadm");
        DatabaseTransactionManager txn = new DatabaseTransactionManager(dataSource);

        try {
            txn.startTransaction();
            keyValue = new KeyValue(name);
            keyValue.value = votable;
            keyValueDAO.put(keyValue);
            this.syncOutput.setCode(HttpURLConnection.HTTP_CREATED);
            txn.commitTransaction();
        } catch (Exception e) {
            log.debug("Error creating descriptor:" + name, e);
            if (txn.isOpen()) {
                try {
                    txn.rollbackTransaction();
                } catch (Exception ex) {
                    log.error("Error rolling back transaction", ex);
                }
            }
            throw new RuntimeException("Error creating descriptor:" + name, e);
        } finally {
            if (txn.isOpen()) {
                log.debug("transaction open in finally");
                try {
                    txn.rollbackTransaction();
                } catch (Exception ex) {
                    log.error("Error rolling back transaction", ex);
                }
            }
        }
    }

}
