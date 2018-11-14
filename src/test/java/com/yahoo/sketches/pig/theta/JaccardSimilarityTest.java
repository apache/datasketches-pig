package com.yahoo.sketches.pig.theta;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.yahoo.sketches.pig.PigTestingUtil.createDbaFromQssRange;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author eshcar
 */
public class JaccardSimilarityTest {

  @Test
  public void checkNullCombinations() throws IOException {
    EvalFunc<Tuple> jaccardFunc = new JaccardSimilarity();

    Tuple inputTuple, resultTuple;
    Double[] est;
    //Two nulls
    inputTuple = TupleFactory.getInstance().newTuple(2);
    resultTuple = jaccardFunc.exec(inputTuple);
    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 3);
    for (Object d : resultTuple.getAll()) {
      assertEquals(d, 0.0);
    }

    //A is null
    inputTuple = TupleFactory.getInstance().newTuple(2);
    inputTuple.set(1, createDbaFromQssRange(256, 0, 128));
    resultTuple = jaccardFunc.exec(inputTuple);
    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 3);
    for (Object d : resultTuple.getAll()) {
      assertEquals(d, 0.0);
    }

    //A is valid, B is null
    inputTuple = TupleFactory.getInstance().newTuple(2);
    inputTuple.set(0, createDbaFromQssRange(256, 0, 256));
    resultTuple = jaccardFunc.exec(inputTuple);
    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 3);
    for (Object d : resultTuple.getAll()) {
      assertEquals(d, 0.0);
    }

    //Both valid
    inputTuple = TupleFactory.getInstance().newTuple(2);
    inputTuple.set(0, createDbaFromQssRange(256, 0, 256));
    inputTuple.set(1, createDbaFromQssRange(256, 0, 128));
    resultTuple = jaccardFunc.exec(inputTuple);
    assertNotNull(resultTuple);
    assertEquals(resultTuple.size(), 3);
    assertEquals(resultTuple.get(0), 0.5);
    assertEquals(resultTuple.get(1), 0.5);
    assertEquals(resultTuple.get(2), 0.5);
  }

}
