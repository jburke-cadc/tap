/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2018.                            (c) 2018.
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
************************************************************************
*/

package ca.nrc.cadc.vosi.actions;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.gms.GroupClient;
import ca.nrc.cadc.gms.GroupURI;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.tap.PluginFactory;
import ca.nrc.cadc.tap.schema.TapSchemaDAO;
import java.net.URI;
import java.security.AccessControlException;
import java.security.Principal;
import javax.security.auth.Subject;
import javax.sql.DataSource;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public abstract class TablesAction extends RestAction {
    private static final Logger log = Logger.getLogger(TablesAction.class);

    
    
    public TablesAction() { 
    }

    protected final DataSource getDataSource() {
        PluginFactory pf = new PluginFactory();
        DataSourceProvider dsf = pf.getDataSourceProvider();
        return dsf.getDataSource(super.syncInput.getRequestPath());
    }
    
    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return null;
    }
    
    String getTableName() {
        String path = syncInput.getPath();
        // TODO: move this empty str to null up to SyncInput?
        if (path != null && path.isEmpty()) {
            return null;
        }
        return path;
    }
    
    /**
     * Create and configure a TapSchemaDAO instance. 
     * 
     * @return 
     */
    protected final TapSchemaDAO getTapSchemaDAO() {
        PluginFactory pf = new PluginFactory();
        TapSchemaDAO dao = pf.getTapSchemaDAO();
        DataSource ds = getDataSource();
        dao.setDataSource(ds);
        dao.setOrdered(true);
        return dao;
    }
    
    Subject getOwner(String name) {
        return Util.getOwner(getDataSource(), name);
    }
    
    void setReadOnlyGroup(String name, URI group) {
        throw new UnsupportedOperationException();
    }
    
    void setReadWriteGroup(String name, URI group) {
        Util.setReadWriteGroup(getDataSource(), name, group);
    }
    
    // schema owner can drop
    // table owner can drop
    // no group permissions used
    void checkDropTablePermission(String tableName) {
        String schemaName = Util.getSchemaFromTable(tableName);
        
        Subject sowner = getOwner(schemaName);
        if (sowner == null) {
            // not listed : no one has permission
            throw new AccessControlException("permission denied");
        }
        Subject towner = getOwner(tableName);
        if (towner == null) {
            // not listed : no one has permission
            throw new AccessControlException("permission denied");
        }
        
        Subject cur = AuthenticationUtil.getCurrentSubject();
        for (Principal cp : cur.getPrincipals()) {
            for (Principal op : sowner.getPrincipals()) {
                if (AuthenticationUtil.equals(op, cp)) {
                    super.logInfo.setMessage("drop allowed: schema owner");
                    return;
                }
            }
            for (Principal op : towner.getPrincipals()) {
                if (AuthenticationUtil.equals(op, cp)) {
                    super.logInfo.setMessage("drop allowed: table owner");
                    return;
                }
            }
        }
        
        throw new AccessControlException("permission denied");
    }
    
    void checkSchemaWritePermission(String schemaName) {
        DataSource ds = getDataSource();
        Subject owner = Util.getOwner(ds, schemaName);
        if (owner == null) {
            // not listed : no one has permission
            throw new AccessControlException("permission denied");
        }
        
        Subject cur = AuthenticationUtil.getCurrentSubject();
        for (Principal cp : cur.getPrincipals()) {
            for (Principal op : owner.getPrincipals()) {
                if (AuthenticationUtil.equals(op, cp)) {
                    return;
                }
            }
        }
        
        // check group write on schema
        URI rwSchemaGroup = Util.getReadWriteGroup(ds, schemaName);
        if (rwSchemaGroup != null) {
            GroupURI groupURI = new GroupURI(rwSchemaGroup);
            URI serviceID = groupURI.getServiceID();
            GroupClient gmsClient = GroupClient.getGroupClient(serviceID);
            if (Util.isMember(gmsClient, rwSchemaGroup)) {
                log.debug("user has schema level (" + schemaName + ") group access via " + rwSchemaGroup);
                return;
            }
        }
        
        throw new AccessControlException("permission denied");
    }
    
    void checkTableWritePermission(String tableName) throws ResourceNotFoundException {
        Util.checkTableWritePermission(getDataSource(), tableName);
    }
    
    void setTableOwner(String tableName, Subject s) {
        Util.setOwner(getDataSource(), tableName, s);
    }
}
