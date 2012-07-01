package com.ramblerag.graphdb;

import org.apache.log4j.Logger;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.tooling.GlobalGraphOperations;

/**
 * Hello world!
 * 
 */
public class App {
	private static final String INDEX_NAME = "nodes";
	private static final String MSG_CREATED_USER = "Created user: %s";
	private static final String USERNAME_KEY = "name";
	private static final String DB_PATH = "var/graphDb";
	private static Logger logger = Logger.getLogger(App.class);

	private GraphDatabaseService graphDb;
	// Index of all nodes
	private Index<Node> nodeIndex;
	// Holds a ref to every user node
	private Node usersReferenceNode;

	public enum RelTypes implements RelationshipType {
		IS_FRIEND_OF, HAS_SEEN, USER
	}

	public static void main(String[] args) {
		new App().execute(args);
	}

	public void execute(String[] args) {
		logger.info("Begin friends graph demo");
		startDb();
		createUsers();

		logger.info("Show friends");
		showAllFriends();
		logger.info("Finished showing friends");

		removeAll();
		shutdownDb();
		logger.info("End friends graph demo");
	}
	
	private void showAllFriends() {
		
		for (Relationship rel : getUsersReferenceNode().getRelationships(RelTypes.USER, Direction.OUTGOING)) {
			
			Node friendNode = rel.getEndNode();
			String friendName = (String) friendNode.getProperty(USERNAME_KEY);
			
			// Yes we could just pass the friendNode in this case, 
			// but, in general, we want showFriends to take a friend name, not a node directly.
			showFriendsOf(friendName);
		}
	}

	private void startDb() {
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
		registerShutdownHook(graphDb);
		nodeIndex = graphDb.index().forNodes(INDEX_NAME);
	}

	private void shutdownDb() {
		graphDb.shutdown();
	}

	private void showFriendsOf(String userName) {
		// Find userName (illustrates finding nodes from index by property)
		// If found, list his friends' names

		// Take first node having property userName. Assumes no users created with same name.
		logger.info(String.format("Friends of %s:", userName));
		Node node = nodeIndex.get(USERNAME_KEY, userName).getSingle();
		
		showFriendsOf(node);
	}
	
	private void showFriendsOf(Node userNode) {

		if (userNode != null) {

			for (Relationship rel : userNode.getRelationships(Direction.OUTGOING)) {
				
				if (rel.isType(RelTypes.IS_FRIEND_OF)) {

					Node friendNode = rel.getEndNode();
					String userName = (String) userNode.getProperty(USERNAME_KEY);
					String friendName = (String) friendNode.getProperty(USERNAME_KEY);
					
					logger.info(String.format("\t%s knows %s", userName, friendName));
				}
			}
		} else {
			logger.info(String.format("User %s invalid", userNode));
		}
	}

	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running example before it's completed)
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}

	private void createUsers() {
		Transaction tx = graphDb.beginTx();
		try {
			Node user1 = createAndIndexUser("Ed Mauget");
			
			Node user2 = createAndIndexUser("Molly Mauget");
			Node user3 = createAndIndexUser("Pixie Mauget");
			Node user4 = createAndIndexUser("Nellie Mauget");

			createFriend(user1, user2);
			createFriend(user1, user3);
			createFriend(user1, user4);
			
			createFriend(user2, user3);
			
			tx.success();
		} catch (Exception e) {
			logger.error(e.toString());
			tx.failure();
		} finally {
			tx.finish();
		}
	}
	
	private void createFriend(Node user1, Node user2){
		user1.createRelationshipTo(user2, RelTypes.IS_FRIEND_OF);
		user2.createRelationshipTo(user1, RelTypes.IS_FRIEND_OF);
	}

	private Node createAndIndexUser(final String username) {
		Node userNode = graphDb.createNode();
		userNode.setProperty(USERNAME_KEY, username);

		nodeIndex.putIfAbsent(userNode, USERNAME_KEY, username);
		getUsersReferenceNode().createRelationshipTo(userNode, RelTypes.USER);

		logger.info(String.format(MSG_CREATED_USER,
				userNode.getProperty(USERNAME_KEY)));
		return userNode;
	}

	private void removeAll() {

		Transaction tx = graphDb.beginTx();
		try {
			GlobalGraphOperations ops = GlobalGraphOperations.at(graphDb);
			
			for (Relationship relationship : ops.getAllRelationships()){
				relationship.delete();
			}
			for (Node node : ops.getAllNodes()){
				node.delete();
			}
			usersReferenceNode = null;
			
			// Index has refs to non-existent nodes. Just remove the index.
			nodeIndex.delete();
			nodeIndex = null;
			
			tx.success();
			logger.info("Deleted all relationships and nodes");
			
		} catch (Exception e) {
			logger.error(e.toString());
			tx.failure();
		} finally {
			tx.finish();
		}
	}

	private Node getUsersReferenceNode() {
		if (null == usersReferenceNode) {
			Transaction tx = graphDb.beginTx();
			try {
				usersReferenceNode = graphDb.createNode();
			} catch (Exception e) {
				logger.error(e.toString());
				tx.failure();
			} finally {
				tx.finish();
			}
		}
		return usersReferenceNode;
	}
}
