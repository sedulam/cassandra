/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.locator;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.tcm.membership.Location;

/**
 * @deprecated See CASSANDRA-19488
 */
@Deprecated(since = "CEP-21")
public class GossipingPropertyFileSnitch extends AbstractNetworkTopologySnitch
{
    private final Location fromConfig;
    public final boolean preferLocal;

    public GossipingPropertyFileSnitch() throws ConfigurationException
    {
        SnitchProperties properties = RackDCFileLocationProvider.loadConfiguration();
        fromConfig = new RackDCFileLocationProvider(properties).initialLocation();
        preferLocal = Boolean.parseBoolean(properties.get("prefer_local", "false"));
    }

    @Override
    public String getLocalRack()
    {
        return fromConfig.rack;
    }

    @Override
    public String getLocalDatacenter()
    {
        return fromConfig.datacenter;
    }

    @Override
    public boolean preferLocalConnections()
    {
        return preferLocal;
    }
}
