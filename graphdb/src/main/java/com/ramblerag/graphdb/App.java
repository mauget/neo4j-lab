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

/**
 * Hello world!
 * 
 */
public class App {
	private static final String INDEX_NAME = "nodes";
	private static final String MSG_DELETED_USER = "Deleted user: %s";
	private static final String MSG_CREATED_USER = "Created user: %s";
	private static final String USERNAME_KEY = "name";
	private static final String DB_PATH = "var/graphDb";
	private static Logger logger = Logger.getLogger(App.class);

	private GraphDatabaseService graphDb;
	private Index<Node> nodeIndex;
	private Node usersReferenceNode;

	public enum RelTypes implements RelationshipType {
		IS_FRIEND_OF, HAS_SEEN, USER, USERS_REFERENCE
	}

	public static void main(String[] args) {
		new App().execute(args);
	}

	public void execute(String[] args) {
		startDb();
		createUsers();

		showFriendsOf("Ed Mauget");
		showFriendsOf("Molly Mauget");
		showFriendsOf("Pixie Mauget");
		showFriendsOf("Nellie Mauget");

		removeAll();
		shutdownDb();
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
		// Find userName
		// If found, list his friends' names

		// Take first node having property userName. Assumes no users created with same name.
		logger.info(String.format("Friends of %s:", userName));
		Node node = nodeIndex.get(USERNAME_KEY, userName).getSingle();
		
		if (node != null) {

			for (Relationship rel : node.getRelationships(Direction.OUTGOING)) {
				
				if (rel.isType(RelTypes.IS_FRIEND_OF)) {

					Node friendNode = rel.getEndNode();
					String friendName = (String) friendNode.getProperty(USERNAME_KEY);
					
					logger.info(String.format("\t%s knows %s", userName,
							friendName));
					// Descend
					//showFriendsOf(friendName);
				}
			}
		} else {
			logger.info(String.format("User %s not found", userName));
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
			// For each user in the user relationshipNode relation:
			for (Relationship relationship : graphDb.getReferenceNode().getRelationships(RelTypes.USER, Direction.OUTGOING)) {
				
				Node user = relationship.getEndNode();
				String name = (String) user.getProperty(USERNAME_KEY);
				
				relationship.delete();
				
				// Sever all ties in either direction
				for (Relationship friendRel : user.getRelationships()){
					friendRel.delete();
				}
				// ... before removing user
				user.delete();

				logger.info(String.format(MSG_DELETED_USER, name));
			}
			
			for (Relationship relationship : graphDb.getReferenceNode().getRelationships()) { //Direction.OUTGOING)) {
				relationship.delete();
			}
			
			// Index now has refs to non-existent nodes. Just remove the index.
			nodeIndex.delete();
			
			usersReferenceNode = null;
			
			tx.success();
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
	            graphDb.getReferenceNode().createRelationshipTo(
	                usersReferenceNode, RelTypes.USERS_REFERENCE );
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
