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
import ca.nrc.cadc.dali.tables.votable.VOTableInfo;
import ca.nrc.cadc.dali.tables.votable.VOTableResource;
import ca.nrc.cadc.dali.tables.votable.VOTableWriter;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.db.version.KeyValueDAO;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.util.StringUtil;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

public abstract class DescriptorAction extends RestAction {
    private static final Logger log = Logger.getLogger(DescriptorAction.class);

    protected static final String VOTABLE_MIME_TYPE = "application/x-votable+xml";

    protected KeyValueDAO keyValueDAO;

    protected DescriptorAction() {
        super();
        init();
    }

    private void init() {
        try {
            DataSource tapadm = DBUtil.findJNDIDataSource("jdbc/tapadm");
            this.keyValueDAO = new KeyValueDAO(tapadm, null, "tap_schema", ServiceDescriptors.class);
        } catch (NamingException e) {
            log.error("Error initializing KeyValueDAO", e);
            throw new IllegalStateException("Error initializing KeyValueDAO", e);
        }
    }

    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return new DescriptorInlineContentHandler();
    }

    protected VOTableDocument getVOTableDocument() {
        VOTableDocument document = (VOTableDocument) syncInput.getContent(DescriptorInlineContentHandler.CONTENT_KEY);
        if (document == null) {
            throw new IllegalArgumentException("No VOTable content found in the request");
        }
        return document;
    }

    protected String getDescriptorID(VOTableDocument document) {
        List<VOTableResource> resources = document.getResources();
        if (resources.size() != 1) {
            throw new IllegalArgumentException("Expected 1 VOTable resource element, found: " + resources.size());
        }

        List<VOTableInfo> infos = resources.get(0).getInfos();
        if (infos.size() != 1) {
            throw new IllegalArgumentException("Expected 1 VOTable info element, found: " + infos.size());
        }

        String value = infos.get(0).getValue();
        if (!StringUtil.hasText(value)) {
            throw new IllegalArgumentException("VOTable info value is empty");
        }
        return value;
    }

    protected String document2String(VOTableDocument document) throws IOException {
        StringWriter stringWriter = new StringWriter();
        VOTableWriter writer = new VOTableWriter();
        writer.write(document, stringWriter);
        return stringWriter.toString();
    }

}
