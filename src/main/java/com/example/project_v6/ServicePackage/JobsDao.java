package com.example.project_v6.ServicePackage;

import com.example.project_v6.JobsPackage.Jobs;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;

import java.util.List;
import java.util.Map;


public interface JobsDao {
    public List<Jobs> getAllJobs();
    public Dataset<Row> getDFJobs();
    public List<List<String>> getDataSummary();
    public StructField [] getDataSchema();
    public List<Row> getMostCompanies();
    public List<Row> getMostTitles();
    public List<Row> getMostLocations();
    public List<Jobs> getJobsBySkills(List<String> skills);
    public List<Jobs> getJobsByLevel(String level);
    public List<Jobs> getJobsByTitle(String title);
    public List<Jobs> getJobsByCompany(String company);
    public List<Map.Entry> getSkillsCount ();

    //Charts

    public void PieChart5(List<Row> Titles_Companies_Most_10_List);
    public void BarChart7(List<Row> Most_10_Titles_List);
    public void BarChart9(List<Row> Most_10_Locations_List);

    //factorization
    public List<Row> factorizeYearsExp();


}
