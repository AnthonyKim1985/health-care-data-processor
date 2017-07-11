package org.bigdatacenter.dataprocessor;

import org.bigdatacenter.dataprocessor.common.DataProcessorUtil;
import org.junit.Test;

//@RunWith(SpringRunner.class)
//@SpringBootTest
//@ContextConfiguration
public class ApplicationTests {
    @Test
    public void contextLoads() {
        String query = "select * from nps.nps_t20_2013;";
        System.out.println(DataProcessorUtil.getHashedString(query));
    }
}