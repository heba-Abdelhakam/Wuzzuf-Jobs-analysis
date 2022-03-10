package com.example.project_v6.controller;

import com.example.project_v6.ServicePackage.JobsDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Controller
public class ExampleController {
    @Autowired
    JobsDao jobsDao;
	
    @RequestMapping("/")
    String MainPage(){
        return "MainPage";
    }
    @RequestMapping("/index")
    String index(){
        return "index";
    }

    @RequestMapping("/job")
    String jobs(Model model){
        model.addAttribute("jobs", jobsDao.getAllJobs().subList(0,50));
        return "job";
    }

    @RequestMapping("/skills")
    String jobs60(Model model){
        List<Map.Entry> skillscount = jobsDao.getSkillsCount();
        List<String> skill=skillscount.stream().map(ls->ls.getKey().toString()).collect(Collectors.toList());
        List<String> count=skillscount.stream().map(ls->ls.getValue().toString()).collect(Collectors.toList());
        Collections.reverse(skill);
        Collections.reverse(count);

        model.addAttribute("skills", skill.subList(0,10));
        model.addAttribute("counts", count.subList(0,10));
        return "skills";
    }

    @RequestMapping("/summary")
    String summary(Model model){
        model.addAttribute("summary", jobsDao.getDataSummary());
        return "summary";
    }

    @RequestMapping("/MostCompanies")
    String companies(Model model){
        model.addAttribute("companies", jobsDao.getMostCompanies());
        return "companies";
    }
    @RequestMapping("/MostTitles")
    String titles(Model model){

        model.addAttribute("titles", jobsDao.getMostTitles());
        return "titles";
    }
    @RequestMapping("/MostLocations")
    String locations(Model model){
        model.addAttribute("locations", jobsDao.getMostLocations());
        return "locations";
    }

    @RequestMapping("/schema")
    String schema(Model model){

        model.addAttribute("schema", jobsDao.getDataSchema());
        return "schema";
    }

    @RequestMapping("/pieChart")
    String piechart(){
        jobsDao.PieChart5(jobsDao.getMostCompanies());
        return "pieChart";
    }

    @RequestMapping("/barChart_1")
    String barChart1(){
        jobsDao.BarChart7(this.jobsDao.getMostTitles());
        return "barChart_1";
    }
    @RequestMapping("/barChart2")
    String barChart2(){
        jobsDao.BarChart9(jobsDao.getMostLocations());
        return "barChart2";
    }

    @RequestMapping("/factorize")
    String factorization(Model  model){
        model.addAttribute("factor", jobsDao.factorizeYearsExp().subList(0,20));
        return "factorize";
    }


}
