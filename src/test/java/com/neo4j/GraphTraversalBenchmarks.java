package com.neo4j;

import org.neo4j.graphdb.*;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class GraphTraversalBenchmarks {
    private static ServerControls neo4j;
    private static ArrayList<Long> items;
    private static ArrayList<Long> people;
    private static RelationshipType LIKES = RelationshipType.withName("LIKES");
    private static final String WEIGHT = "weight";

    private Random rand = new Random();

    @Param({"100000"})
    private int personCount;

    @Param({"2000"})
    private int itemCount;

    @Param({"100"})
    private int likesCount;

    @Setup
    public void prepare() throws IOException {
        items = new ArrayList<>();
        people = new ArrayList<>();

        neo4j = TestServerBuilders.newInProcessBuilder()
                .newServer();

        GraphDatabaseService db = neo4j.graph();
        Transaction tx = db.beginTx();
        org.neo4j.graphdb.schema.Schema schema = db.schema();

        if (!schema.getIndexes(Label.label("Item")).iterator().hasNext()) {
            schema.constraintFor(Label.label("Item"))
                    .assertPropertyIsUnique("id")
                    .create();
        }

        if (!schema.getIndexes(Label.label("Person")).iterator().hasNext()) {
            schema.constraintFor(Label.label("Person"))
                    .assertPropertyIsUnique("id")
                    .create();
        }
        tx.success();
        tx.close();
        tx = db.beginTx();

        for (int item = 0; item < itemCount; item++) {
            HashMap<String, Object> properties = new HashMap<>();
            properties.put("id", item);
            properties.put("itemname", "itemname" + item );
            Node node = db.createNode(Label.label("Item"));
            node.setProperty("item", item);
            properties.entrySet().forEach( (entry) -> {
                node.setProperty(entry.getKey(), entry.getValue());
            });
            items.add(node.getId());
        }

        for (long person = 0; person < personCount; person++) {
            Node node = db.createNode(Label.label("Person"));
            node.setProperty("person", person);
            people.add(node.getId());

            Random rand = new Random();
            if (rand.nextInt(100) == 5) {
                tx.success();
                tx.close();
                tx = db.beginTx();
            }
            for (long like = 0; like < likesCount; like++) {
                Long randomItem = items.get(rand.nextInt(items.size()));
                Relationship r = node.createRelationshipTo(db.getNodeById(randomItem), RelationshipType.withName("LIKES"));
                r.setProperty(WEIGHT, rand.nextDouble() * 10.0);
            }
        }
        tx.success();
        tx.close();
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(1)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public long measureRecommendationTraversal() {
        long[] count = new long[]{0};
        GraphDatabaseService db = neo4j.graph();
        Transaction tx = db.beginTx();

        Long randomPerson = people.get(rand.nextInt(people.size()));
        Node person = db.getNodeById(randomPerson);

        person.getRelationships(Direction.OUTGOING, RelationshipType.withName("LIKES")).forEach(rel -> {
            long itemId = rel.getEndNodeId();
            Node item = db.getNodeById(itemId);
            item.getRelationships(Direction.INCOMING, RelationshipType.withName("LIKES")).forEach(rel2 -> {
                long otherPersonId = rel2.getStartNodeId();
                Node otherPerson = db.getNodeById(otherPersonId);
                otherPerson.getRelationships(Direction.OUTGOING, RelationshipType.withName("LIKES")).forEach(rel3 ->
                        count[0]++);
            });
        });

        return count[0];
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 10)
    @Fork(1)
    @Threads(1)
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public long measureRecommendationTraversalWithRelationshipProperties() {
        long[] count = new long[]{0};
        GraphDatabaseService db = neo4j.graph();
        Transaction tx = db.beginTx();

        Long randomPerson = people.get(rand.nextInt(people.size()));
        Node person = db.getNodeById(randomPerson);

        person.getRelationships(Direction.OUTGOING, LIKES).forEach(rel -> {
            if ((double)rel.getProperty(WEIGHT) > -1.0) {
                long itemId = rel.getEndNodeId();
                Node item = db.getNodeById(itemId);
                item.getRelationships(Direction.INCOMING, LIKES).forEach(rel2 -> {
                    if ((double)rel2.getProperty(WEIGHT) > -1.0) {
                        long otherPersonId = rel2.getStartNodeId();
                        Node otherPerson = db.getNodeById(otherPersonId);
                        otherPerson.getRelationships(Direction.OUTGOING, LIKES).forEach(rel3 -> {
                            if ((double)rel3.getProperty(WEIGHT) > -1.0) {
                                count[0]++;
                            }
                    });
                }});
            }
        });

        return count[0];

    }
}
