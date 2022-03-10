package com.example.project_v6.ServicePackage;//// imports of Spark
//import org.apache.commons.lang3.Range;
//import org.apache.spark.ml.feature.StringIndexer;
//import org.apache.spark.sql.DataFrameReader;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//
//
////import for xchart
//import org.knowm.xchart.*;
//import org.knowm.xchart.style.Styler;
//
//import java.awt.*;
//import java.util.Arrays;
//import java.util.List;
//import java.util.stream.Collectors;
//import java.util.stream.IntStream;
//
//
//public class WuzzufMain<JOBS> {
//    public static void main(String[] args) {
//        JobsDAO_ImpCSV Job_DAO = new JobsDAO_ImpCSV();
//        List<Jobs> JOBS = Job_DAO.readJobFromCSV("C:\\Users\\HeBa\\IdeaProjects\\Project\\src\\main\\resources\\Wuzzuf_Jobs.csv");
///*       for(Jobs J : JOBS)
//        {
//            System.out.println(J);
//        }*/
//
//        // read Data By Using Spark
//        // Create Spark Session to create connection to Spark
//        final SparkSession sparkSession = SparkSession.builder ().appName("WUZZUF Demo").master ("local[6]").getOrCreate ();
//        // Get DataFrameReader using SparkSession
//        final DataFrameReader dataFrameReader = sparkSession.read ();
//        // Set header option to true to specify that first row in file contains name of columns
//        dataFrameReader.option ("header", "true");
//        //
//        Dataset<Row> WuzzJob = dataFrameReader.csv ("C:\\Users\\HeBa\\IdeaProjects\\Project\\src\\main\\resources\\Wuzzuf_Jobs.csv");
//        // Print Schema to see column names, types and other metadata
////        WuzzJob.printSchema ();
////        System.out.println("-------------------------------------------------------");
////        WuzzJob.show();
//        //WuzzJob = WuzzJob.select("Title", "Company", "Location");
//        System.out.println("Number of records = "+WuzzJob.count());
//        //WuzzJob.show();
///*        //drop null values
//        Dataset<Row> WuzzJob_NoNull = WuzzJob.na().drop();
//        WuzzJob_NoNull.printSchema();
//        System.out.println(WuzzJob_NoNull.count());
//        System.out.println("====================================================================");
//        WuzzJob.describe().show();
//        System.out.println("====================================================================");*/
//
//        //// Create view to write SQL ////
//        // Register the DataFrame as a SQL temporary vie    w
//        WuzzJob.createOrReplaceTempView("WuzzufJob");
//        Dataset<Row> Titles_Companies = sparkSession.sql("SELECT COUNT(Title) as Num_required_Jobs, Company FROM WuzzufJob GROUP BY Company ORDER BY COUNT(Title) DESC");
// //       Titles_Companies.show();
//        System.out.println("******************************************************************************************************************");
//
//        // Extract Most 10 Companies that reqired jobs and transfer it to list
//        List<Row> Titles_Companies_Most_10 = Titles_Companies.limit(10).collectAsList();
//         //System.out.println(Titles_Companies_Most_10);
//
//
//        //// extract the most popular job titles ////
//        Dataset<Row> Most_Titles = sparkSession.sql("SELECT COUNT(Title) as Num_required_Jobs, Title FROM WuzzufJob GROUP BY Title ORDER BY COUNT(Title) DESC");
// //       Most_Titles.show();
//
//        List<Row> Most_10_Titles = Most_Titles.limit(10).collectAsList();
//
//
//        ///// Extract the most popular areas  /////
//        Dataset<Row> Most_Locations = sparkSession.sql("SELECT COUNT(Location) as Num_Locations, Location FROM WuzzufJob GROUP BY Location ORDER BY COUNT(Location) DESC");
//        Most_Locations.show();
//
//        List<Row> Most_10_Locations = Most_Locations.limit(10).collectAsList();
//
//        /////  Charts /////
//        WuzzufMain xChartExamples = new WuzzufMain ();
//        // Pie Chart
//        xChartExamples.PieChart5(Titles_Companies_Most_10);
//        xChartExamples.BarChart7(Most_10_Titles);
//
//        xChartExamples.BarChart9(Most_10_Locations);
//
//
//
//        ////  Factorize the YearsExp feature and convert it to numbers in new column ////
//        StringIndexer indexer = new StringIndexer();
//        indexer.setInputCol("YearsExp").setOutputCol("YearsExp. indexed");
//        Dataset<Row> data_with_newColumn = indexer.fit(WuzzJob).transform(WuzzJob);
//        // drop Columns
//        data_with_newColumn = data_with_newColumn.drop("YearsExp");
//        data_with_newColumn.show();
//
//        System.out.println("******************************************************************************8");
//        System.out.println(Titles_Companies_Most_10);
//    }
//
//
//    // Method for PieChart in Q.5
//    public void PieChart5(List<Row> Titles_Companies_Most_10_List){
//
//        // Create Chart
//        PieChart chart = new PieChartBuilder().width(2000).height (800).title("Most 10 Companies required Jobs").build();
//
//        int size = Titles_Companies_Most_10_List. size();
//        System.out.println("Titles Size = "+size);
//
//        // Customize Chart
//        chart.getStyler ().setLegendPosition (Styler.LegendPosition.InsideNE);
//
//        for(Row t : Titles_Companies_Most_10_List)
//        {
//            //Will extract first element from row --> ( number of jobs required at the company)
//            // and second element in the row ----> (Name of the Company)
//            chart.addSeries ( (String) t.get(1), (Number) t.get(0));
//        }
//
//        // Show it
//        new SwingWrapper (chart).displayChart ();
//    }
//
//    // Method for BarChart in Q.7
//    public void BarChart7(List<Row> Most_10_Titles_List){
//
//        //System.out.println(Most_10_Titles_List);
//        // extract column of titles numbers (Column 0) from input List
//        List<Long> Titles_nums = Most_10_Titles_List.stream().map(ls->(Long)ls.get(0)).collect(Collectors.toList());
//        // extract column of titles names (Column 1 ) from input List
//        List<String> Titles_names = Most_10_Titles_List.stream().map(ls->ls.get(1).toString()).collect(Collectors.toList());
//        System.out.println(Titles_nums);
//        System.out.println(Titles_names);
//
//        // Create Chart
//        CategoryChart chart = new CategoryChartBuilder().width (2000).height (800).title ("The Most Popular Jobs").xAxisTitle ("Names Of Jobs").yAxisTitle ("Numbers").build ();
//
//        // Customize Chart
//        Color[] sliceColors = new Color[] { new Color(173, 143, 61)};
//        chart.getStyler().setSeriesColors(sliceColors);
//        chart.getStyler ().setLegendPosition (Styler.LegendPosition.InsideNE);
//        chart.getStyler ().setHasAnnotations (true);
//        chart.getStyler ().setStacked (true);
//        chart.getStyler().setXAxisLabelRotation(45);
//
//        chart.addSeries("Job Titles",Titles_names, Titles_nums);
//
//        // Show it
//        new SwingWrapper (chart).displayChart ();
//    }
//
//    // Method for BarChart in Q.9
//    public void BarChart9(List<Row> Most_10_Locations_List){
//
//        // extract column of Locations numbers (Column 0) from input List
//        List<Long> Locations_nums = Most_10_Locations_List.stream().map(ls->(Long)ls.get(0)).collect(Collectors.toList());
//        // extract column of Locations names (Column 1 ) from input List
//        List<String> Locations_names = Most_10_Locations_List.stream().map(ls->ls.get(1).toString()).collect(Collectors.toList());
//        System.out.println(Locations_nums);
//        System.out.println(Locations_names);
//
//        // Create Chart
//        CategoryChart chart = new CategoryChartBuilder().width (1024).height (768).title ("The Most Popular Locations").xAxisTitle ("Names Of Locations").yAxisTitle ("Numbers").build ();
//
//        // Customize Chart
//        Color[] sliceColors = new Color[] { new Color(40, 165, 168)};
//        chart.getStyler().setSeriesColors(sliceColors);
//        chart.getStyler ().setLegendPosition (Styler.LegendPosition.InsideNE);
//        chart.getStyler ().setHasAnnotations (true);
//        chart.getStyler ().setStacked (true);
//
//        chart.addSeries("Locations",Locations_names, Locations_nums);
//
//        // Show it
//        new SwingWrapper (chart).displayChart ();
//    }
//
//
//}
