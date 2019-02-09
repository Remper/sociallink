package eu.fbk.fm.alignments.query.index;

import eu.fbk.fm.alignments.kb.DBpediaSpec;
import eu.fbk.fm.alignments.kb.KBResource;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Testing various query construction options
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class AllNamesStrategyTest {

    @Test
    public void getQuery() {

        List<String> names = new LinkedList<>();
        names.add("test_name");
        names.add("test_name");
        names.add("test_name");
        names.add("test_name");
        names.add("test_name2");
        names.add("test_name2");
        names.add("test_name2");
        names.add("test_name3");
        names.add("test_name3");
        names.add("test_name4");
        names.add("test_name5");
        names.add("test_name5");
        names.add("test_name5");
        names.add("test_name5");
        names.add("test_name5");
        names.add("test_name5");

        List<String> givenNames = new LinkedList<>();
        givenNames.add("test_name5");

        Map<String, List<String>> attributes = new HashMap<>();
        attributes.put(DBpediaSpec.ATTRIBUTE_NAME, names);
        attributes.put(DBpediaSpec.ATTRIBUTE_GIVEN_NAME, givenNames);
        KBResource resource = new KBResource("test", new DBpediaSpec(), attributes);

        AllNamesStrategy strategy = new AllNamesStrategy();

        assertEquals("('test_name') | ('test_name2') | ('test_name3')", strategy.getQuery(resource));
        assertEquals("('test_name') | ('test_name2') | ('test_name3')", strategy.getQuery(resource, 0));
        assertEquals("('test_name') | ('test_name2')", strategy.getQuery(resource, 1));
        assertEquals("'test_name'", strategy.getQuery(resource, 2));
        assertEquals("'test_name'", strategy.getQuery(resource, 3));
    }
}