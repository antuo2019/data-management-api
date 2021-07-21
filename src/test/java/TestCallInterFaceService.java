
import com.bayee.petition.service.CallInterfaceService;
import com.bayee.petition.service.impl.CallInterfaceServiceImpl;

import org.junit.Test;


/**
 * @ClassName TestCallInterFaceService
 * @Description: TODO
 * @Author Lyb
 * @Date 2021/3/11
 **/

public class TestCallInterFaceService {

    String maxLon = "123.164255926266";
    String minLon = "121.55735919936842";
    String minLat = "30.46316371126636";
    String maxLat = "31.003865345776802";
    String precision = "3";
    String mmsi = "413236670";
    String keyWord = "imo";
    String startTime = "1553099142245";
    String endTime = "1553185542245";
    String code = "T063";


    CallInterfaceService callInterfaceService = new CallInterfaceServiceImpl();

    @Test
    public void testAetAisMap(){
        System.out.println("### response: "+callInterfaceService.getAisMap(minLon, maxLon, minLat, maxLat, precision));
    }

    @Test
    public void testShipListData(){
        System.out.println("### response: "+callInterfaceService.shipListData(minLon, maxLon, minLat, maxLat));
    }

    @Test
    public void testGetShipInfo(){
        System.out.println("### response: "+callInterfaceService.getShipInfo(mmsi));
    }

    @Test
    public void testSearchShip(){
        System.out.println("### response: "+callInterfaceService.searchShip(keyWord));
    }

    @Test
    public void testShipTrajectoryByHbase() {
        System.out.println("### response: "+callInterfaceService.shipTrajectoryByHbase("412357170",null,null));
    }

    @Test
    public void testGetMyPorts(){
        System.out.println("### response: "+callInterfaceService.getMyPorts());
    }

    @Test
    public void testGetTideByPort(){
        System.out.println("### response: "+callInterfaceService.getTideByPort(code));
    }

    @Test
    public void testFindComplexByYearOrName(){
        System.out.println("### response: "+callInterfaceService.findComplexByYearOrName("2018",null));
    }

    @Test
    public void testFindComplexNow(){
        System.out.println("### response: "+callInterfaceService.findComplexNow());
    }

    @Test
    public void testFindComplexByComplexId(){
        System.out.println("### response: "+callInterfaceService.findComplexByComplexId("1"));
    }

    @Test
    public void testHarbourType(){
        System.out.println("### response: "+callInterfaceService.harbourType());
    }

    @Test
    public void testHarbour(){
        System.out.println("### response: "+callInterfaceService.harbour("1"));
    }

    @Test
    public void testOrderLine(){
        System.out.println("### response: "+callInterfaceService.orderLine());
    }

    @Test
    public void testFind(){
        System.out.println("### response: "+callInterfaceService.find("E-0002232","北部湾港防城港域渔万港区","E-0008538","北部湾港钦州港域内贸码头","120","8.7","3",null));
    }

    @Test
    public void testGetNavigation(){
        System.out.println("### response: "+callInterfaceService.getNavigation(code,null));
    }

    @Test
    public void testGetNavigationInfoEsId(){
        System.out.println("### response: "+callInterfaceService.getNavigationInfoEsId("D880F847-363A-40E8-9540-4A0C12E62A25_2"));
    }

    @Test
    public void testFindComplexYear(){
        System.out.println("### response: "+callInterfaceService.findComplexYear());
    }

    @Test
    public void testFindComplexByInfo(){
        System.out.println("### response: "+callInterfaceService.findComplexByInfo("201829"));
    }

    @Test
    public void testFindComplexByCitys(){
        System.out.println("### response: "+callInterfaceService.findComplexByCitys("201829"));
    }

    @Test
    public void testGetWindAreaDataList(){
        System.out.println("### response: "+callInterfaceService.getWindAreaDataList());
    }

    @Test
    public void testGetWindAreaDetailInfo(){
        System.out.println("### response: "+callInterfaceService.getWindAreaDetailInfo("20210311/00.json"));
    }

    @Test
    public void testGetManualForecastByPaged(){
        System.out.println("### response: "+callInterfaceService.getManualForecastByPaged("1","10"));
    }

    @Test
    public void testGetManualForecastById(){
        System.out.println("### response: "+callInterfaceService.getManualForecastById("2099"));
    }

    @Test
    public void testCx(){
        System.out.println("### response: "+callInterfaceService.cx("a4617119154712324770"));
    }

    @Test
    public void testGetShipInfoListByMMSIFromHbase(){
        System.out.println("### response: "+callInterfaceService.getShipInfoListByMMSIFromHbase("412357170,412423550"));
    }

    @Test
    public void testGetAllPubNameByType(){
        System.out.println("### response: "+callInterfaceService.getAllPubNameByType("2"));
    }

    @Test
    public void testGetNavigationsInfosByPage(){
        System.out.println("### response: "+callInterfaceService.getNavigationsInfosByPage("2","三亚海事局"));
    }

}
