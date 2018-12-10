package org.apache.cassandra.gms;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * LeaderElection
 */
public class LeaderElection implements Watcher
{

    private final String PATH = "/election";
    private static final Logger logger = LoggerFactory.getLogger(LeaderElection.class);

    private ZooKeeper zk;
    private String proposal;

    public LeaderElection() throws IOException
    {
        zk = new ZooKeeper("vm1", 3000, this);
    }

    public long getLeader() throws KeeperException, InterruptedException
    {
        // check election zknode and create if not exists
        Stat stat = zk.exists(PATH, false);
        if (stat == null)
        {
            logger.debug("Creating znode for leader election");
            String r = zk.create(PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            logger.debug("znode is created: " + r);
        }

        if (proposal == null)
            propose();

        List<String> children = zk.getChildren(PATH, false);

        String leaderProposal = children.get(0);
        for (int i = 1; i < children.size(); i++)
        {
            if (children.get(i).compareTo(leaderProposal) < 0)
            {
                leaderProposal = children.get(i);
            }
        }

        leaderProposal = PATH + "/" + leaderProposal;
        stat = zk.exists(leaderProposal, true);
        return stat.getEphemeralOwner();
    }

    public boolean isLeader() throws KeeperException, InterruptedException
    {
        return zk.getSessionId() == getLeader();
    }

    private void propose() throws KeeperException, InterruptedException
    {
        String newProposal = PATH + "/n_";
        newProposal = zk.create(newProposal, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("A proposal znode is created: " + newProposal);
        proposal = newProposal;
    }

    public void process(final WatchedEvent watchedEvent)
    {
        logger.debug("Watched event: " + watchedEvent.toString());
        // TODO
    }
}
