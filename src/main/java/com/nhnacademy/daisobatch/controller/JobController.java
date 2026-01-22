package com.nhnacademy.daisobatch.controller;

import java.time.LocalDateTime;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class JobController {
    /**
     * 테스트용 컨트롤러~~~!!!!!
     * 즉시 배치 작업 확인하고 싶을 때 이거 쓰기~~
     */

    private final JobLauncher jobLauncher;

    private final Job birthdayCouponJobMSA;
    private final Job birthdayCouponJobDB;

    private final Job dormantAccountJob;

    private final Job gradeChangeJob;

    public JobController(
            JobLauncher jobLauncher,
            @Qualifier("birthdayCouponJobMSA") Job birthdayCouponJobMSA,
            @Qualifier("birthdayCouponJobDB") Job birthdayCouponJobDB,
            @Qualifier("dormantAccountJob") Job dormantAccountJob,
            @Qualifier("gradeChangeJob") Job gradeChangeJob
    ) {
        this.jobLauncher = jobLauncher;
        this.birthdayCouponJobMSA = birthdayCouponJobMSA;
        this.birthdayCouponJobDB = birthdayCouponJobDB;
        this.dormantAccountJob = dormantAccountJob;
        this.gradeChangeJob = gradeChangeJob;
    }

    // 생일 쿠폰 - MSA
    @GetMapping("/batch/birthday/msa")
    public String runBirthdayMsaJob() {
        long start = System.currentTimeMillis();
        try {
            JobParameters params = new JobParametersBuilder()
                    .addLong("runDate", System.currentTimeMillis())
                    .toJobParameters();

            var exec = jobLauncher.run(birthdayCouponJobMSA, params);

            long sec = (System.currentTimeMillis() - start) / 1000;
            return String.format("생일 쿠폰(MSA) 배치 완료! status=%s, time=%d초", exec.getStatus(), sec);

        } catch (Exception e) {
            e.printStackTrace();
            return "생일 쿠폰(MSA) 배치 실패: " + e.getMessage();
        }
    }

    // 생일 쿠폰 - DB
    @GetMapping("/batch/birthday/db")
    public String runBirthdayDbJob() {
        long start = System.currentTimeMillis();
        try {
            JobParameters params = new JobParametersBuilder()
                    .addLong("runDate", System.currentTimeMillis())
                    .toJobParameters();

            var exec = jobLauncher.run(birthdayCouponJobDB, params);

            long sec = (System.currentTimeMillis() - start) / 1000;
            return String.format("생일 쿠폰(DB) 배치 완료! status=%s, time=%d초", exec.getStatus(), sec);

        } catch (Exception e) {
            log.error("생일 쿠폰(DB) 배치 실패", e);
            return "생일 쿠폰(DB) 배치 실패: " + e.getMessage();
        }
    }

    @GetMapping("/batch/dormant")
    public String runDormantJob() {     // 휴면 계정 전환 배치 작업
        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString("baseDate", LocalDateTime.now().toString())
                    .toJobParameters();

            jobLauncher.run(dormantAccountJob, jobParameters);

            return "휴면 계정 전환 배치 작업 완료";

        } catch (Exception e) {
            log.error("휴면 계정 전환 배치 실패", e);
            return "배치 실행 실패: " + e.getMessage();
        }
    }

    @GetMapping("/batch/grade")
    public String runGradeJob() {   // 등급 변경 배치 작업
        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString("baseDate", LocalDateTime.now().toString())
                    .toJobParameters();

            jobLauncher.run(gradeChangeJob, jobParameters);

            return "등급 변경 배치 작업 완료";

        } catch (Exception e) {
            log.error("등급 변경 배치 실패", e);
            return "배치 실행 실패: " + e.getMessage();
        }
    }

}
