package com.hadooptraining.test.recommender;

import java.util.Arrays;
import java.util.Collection;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class AtomDataAsJsonTest extends TestCase {
	
	String atomPath;
	// AtomDataAsJson atom;
	
	public AtomDataAsJsonTest(String atompath) {
		super();
		this.atomPath = atompath;
		// atom = new AtomDataAsJson(this.atomPath, AtomDataAsJson.SASSLOCATION.DEV);
	}

	@Test
	public void testJson() {
		try {
			System.out.println("Atom path = " + this.atomPath);
			// System.out.println("JSON Representation = " + atom.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		// assertEquals("7865258", orderid);
	}

	@Test
	public void testParent() {
		try {
			// System.out.println("Parent = " + atom.getParent());
		} catch (Exception e) {
			e.printStackTrace();
		}
		// assertEquals("7865258", orderid);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		testJson();
		testParent();
	}
	
	@Parameters
	public static Collection data() {
		Object[][] data = new Object[][] { 
			{ "/pnas.atom" } , 
			{ "/demoptjournal/91/6.atom" } 
		};
		return Arrays.asList(data);
	}
}

