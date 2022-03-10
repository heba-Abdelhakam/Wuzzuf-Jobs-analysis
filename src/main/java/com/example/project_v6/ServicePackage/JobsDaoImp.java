package com.example.project_v6.ServicePackage;

import com.example.project_v6.JobsPackage.Jobs;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;
import org.springframework.stereotype.Service;

import java.awt.*;
import java.io.IOException;
import java.util.List;
import java.util.*;
import java.util.stream.Collectors;


@Service
public class JobsDaoImp implements JobsDao {
    private Dataset<Row> dfJobs;
    private List<Jobs> lsJobs;
    private final SparkSession sparkSession = SparkSession.builder ().appName ("Spark CSV Analysis Demo").master ("local[2]")
            .getOrCreate ();


    public JobsDaoImp() {


    }

    @Override
    public List<Jobs> getAllJobs() {

        lsJobs = new ArrayList<>();
        if (dfJobs == null){
            dfJobs = this.getDFJobs();
        }
        List<Row> lsj = dfJobs.collectAsList();
        lsj.stream().forEach(rec->{
            String title = rec.getAs("Title").toString();
            String company = rec.getAs ("Company").toString();
            String location = rec.getAs ("Location").toString();
            String type = rec.getAs ("Type").toString();
            String level = rec.getAs ("Level").toString();
            String yearsExp = rec.getAs ("YearsExp").toString();
            String country = rec.getAs ("Country").toString();
            List<String> skills= new ArrayList<>(Arrays.asList(rec.getAs ("Skills").toString().split(",")));
            Jobs job = new Jobs(title, company, location, type, level, yearsExp,country,skills);
            lsJobs.add (job);
        });

        return lsJobs;
    }

    @Override
    public Dataset<Row> getDFJobs() {
        if (dfJobs != null){
            return this.dfJobs;
        }

        // Get DataFrameReader using SparkSession
        final DataFrameReader dataFrameReader = sparkSession.read ();
        // Set header option to true to specify that first row in file contains
        // name of columns
        dataFrameReader.option ("header", "true");
        dfJobs = dataFrameReader.csv ("src/main/resources/Wuzzuf_Jobs.csv");
//        this.dfJobs = df.as(Encoders.bean(Jobs.class));
        dfJobs.summary();
        return this.dfJobs;
    }

    @Override
    public List<List<String>> getDataSummary() {
        if (dfJobs == null){
            dfJobs = this.getDFJobs();
        }
        List<Row> summary = this.dfJobs.summary().collectAsList();
        List<List<String>> sum = summary.stream().map(rec-> Arrays.asList(rec.toString().split(","))).collect(Collectors.toList());
        return sum;
    }

    @Override
    public StructField [] getDataSchema() {
        if (dfJobs == null){
            this.dfJobs = this.getDFJobs();
        }
//        return dfJobs.schema().map(ls-> Arrays.asList(ls.toString().split(",")).toString());
        return this.dfJobs.schema().fields();
//        List<String> summary = Arrays.asList(dfJobs.schema().simpleString().split(","));
//        List<List<String>> sum = summary.toStream().map(rec-> Arrays.asList(rec.toString().split(","))).collect(Collectors.toList());

    }

    @Override
    public List<Row> getMostCompanies() {
        if (dfJobs == null){
            this.dfJobs = this.getDFJobs();
        }
        dfJobs.createOrReplaceTempView("WuzzufJob");
        Dataset<Row> Titles_Companies = sparkSession.sql("SELECT COUNT(Title) as Num_required_Jobs, Company FROM WuzzufJob GROUP BY Company ORDER BY COUNT(Title) DESC");
        //       Titles_Companies.show();
        System.out.println("******************************************************************************************************************");

        // Extract Most 10 Companies that reqired jobs and transfer it to list
        List<Row> Titles_Companies_Most_10 = Titles_Companies.limit(10).collectAsList();
        return Titles_Companies_Most_10;
    }

    @Override
    public List<Row> getMostTitles() {
        if (dfJobs == null){
            this.dfJobs = this.getDFJobs();
        }
        dfJobs.createOrReplaceTempView("WuzzufJob");
        Dataset<Row> Most_Titles = sparkSession.sql("SELECT COUNT(Title) as Num_required_Jobs, Title FROM WuzzufJob GROUP BY Title ORDER BY COUNT(Title) DESC");
        //       Most_Titles.show();

        List<Row> Most_10_Titles = Most_Titles.limit(10).collectAsList();

        return Most_10_Titles;
    }

    @Override
    public List<Row> getMostLocations() {
        if (dfJobs == null){
            this.dfJobs = this.getDFJobs();
        }
        dfJobs.createOrReplaceTempView("WuzzufJob");
        Dataset<Row> Most_Locations = sparkSession.sql("SELECT COUNT(Location) as Num_Locations, Location FROM WuzzufJob GROUP BY Location ORDER BY COUNT(Location) DESC");
        Most_Locations.show();

        List<Row> Most_10_Locations = Most_Locations.limit(10).collectAsList();
        return Most_10_Locations;
    }

    @Override
    public List<Jobs> getJobsBySkills(List<String> skills) {
        List<Jobs> jobs = lsJobs.stream().filter(job->job.getSkills().contains(skills)).collect(Collectors.toList());
        return jobs;
    }

    @Override
    public List<Jobs> getJobsByLevel(String level) {
        List<Jobs> jobs = lsJobs.stream().filter(job->job.getLevel().equals(level)).collect(Collectors.toList());
        return jobs;
    }

    @Override
    public List<Jobs> getJobsByTitle(String title) {
        List<Jobs> jobs = lsJobs.stream().filter(job->job.getTitle().equals(title)).collect(Collectors.toList());
        return jobs;
    }

    @Override
    public List<Jobs> getJobsByCompany(String company) {
        List<Jobs> jobs = lsJobs.stream().filter(job->job.getCompany().equals(company)).collect(Collectors.toList());
        return jobs;
    }



    @Override
    public List<Map.Entry> getSkillsCount() {

        if (this.lsJobs == null){
            this.lsJobs = this.getAllJobs();
        }
        List<String> skills = this.lsJobs.stream().map(job->job.getSkills()).collect(Collectors.toList()).stream().flatMap(List::stream).collect(Collectors.toList());
        Map<String, Long> skillcount = new HashMap<>();
        skills.forEach(skill-> skillcount.compute(skill,(K,v) ->v==null ? 1L : v + 1L));
        List<Map.Entry> sortedskills = skillcount.entrySet().stream().sorted(Map.Entry.comparingByValue()).collect(Collectors.toList());


        return sortedskills;
    }

    // Method for PieChart in Q.5
    public void PieChart5(List<Row> Titles_Companies_Most_10_List) {
        try
        {        // Create Chart
            PieChart chart = new PieChartBuilder().width(2000).height(800).title("Most 10 Companies required Jobs").build();

            int size = Titles_Companies_Most_10_List.size();
            System.out.println("Titles Size = " + size);

            // Customize Chart
            chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNE);

            for (Row t : Titles_Companies_Most_10_List) {
                //Will extract first element from row --> ( number of jobs required at the company)
                // and second element in the row ----> (Name of the Company)
                chart.addSeries((String) t.get(1), (Number) t.get(0));
            }

            // Show it
//            new SwingWrapper(chart).displayChart();
            BitmapEncoder.saveBitmap(chart, "src/main/resources/static/img/PieChart", BitmapEncoder.BitmapFormat.JPG);
        }
        catch(IOException e){
            e.printStackTrace();
        }
    }

    // Method for BarChart in Q.7
    public void BarChart7(List<Row> Most_10_Titles_List){
        try
        {
            //System.out.println(Most_10_Titles_List);
            // extract column of titles numbers (Column 0) from input List
            List<Long> Titles_nums = Most_10_Titles_List.stream().map(ls->(Long)ls.get(0)).collect(Collectors.toList());
            // extract column of titles names (Column 1 ) from input List
            List<String> Titles_names = Most_10_Titles_List.stream().map(ls->ls.get(1).toString()).collect(Collectors.toList());
//            System.out.println(Titles_nums);
//            System.out.println(Titles_names);

            // Create Chart
            CategoryChart chart = new CategoryChartBuilder().width (2000).height (800).title ("The Most Popular Jobs").xAxisTitle ("Names Of Jobs").yAxisTitle ("Numbers").build ();

            // Customize Chart
            Color[] sliceColors = new Color[] { new Color(173, 143, 61)};
            chart.getStyler().setSeriesColors(sliceColors);
            chart.getStyler ().setLegendPosition (Styler.LegendPosition.InsideNE);
            chart.getStyler ().setHasAnnotations (true);
            chart.getStyler ().setStacked (true);
            chart.getStyler().setXAxisLabelRotation(45);

            chart.addSeries("Job Titles",Titles_names, Titles_nums);

            // Show it
//            new SwingWrapper (chart).displayChart ();
            BitmapEncoder.saveBitmap(chart, "src/main/resources/static/img/BarChart_1", BitmapEncoder.BitmapFormat.JPG);
        }
        catch(IOException e){
            e.printStackTrace(); }
    }

    // Method for BarChart in Q.9
    public void BarChart9(List<Row> Most_10_Locations_List) {
        try {
            // extract column of Locations numbers (Column 0) from input List
            List<Long> Locations_nums = Most_10_Locations_List.stream().map(ls -> (Long) ls.get(0)).collect(Collectors.toList());
            // extract column of Locations names (Column 1 ) from input List
            List<String> Locations_names = Most_10_Locations_List.stream().map(ls -> ls.get(1).toString()).collect(Collectors.toList());
//            System.out.println(Locations_nums);
//            System.out.println(Locations_names);

            // Create Chart
            CategoryChart chart = new CategoryChartBuilder().width(1024).height(768).title("The Most Popular Locations").xAxisTitle("Names Of Locations").yAxisTitle("Numbers").build();

            // Customize Chart
            Color[] sliceColors = new Color[]{new Color(40, 165, 168)};
            chart.getStyler().setSeriesColors(sliceColors);
            chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNE);
            chart.getStyler().setHasAnnotations(true);
            chart.getStyler().setStacked(true);

            chart.addSeries("Locations", Locations_names, Locations_nums);

            // Show it
//            new SwingWrapper(chart).displayChart();
            BitmapEncoder.saveBitmap(chart, "src/main/resources/static/img/BarChart_2", BitmapEncoder.BitmapFormat.JPG);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Row> factorizeYearsExp() {
        if (dfJobs == null){
            dfJobs = this.getDFJobs();
        }
        StringIndexer indexer = new StringIndexer();
        indexer.setInputCol("YearsExp").setOutputCol("YearsExp. indexed");
        Dataset<Row> data_with_newColumn = indexer.fit(dfJobs).transform(dfJobs);
        // drop Columns
        data_with_newColumn = data_with_newColumn.drop("YearsExp");
        return data_with_newColumn.collectAsList();

    }
}
