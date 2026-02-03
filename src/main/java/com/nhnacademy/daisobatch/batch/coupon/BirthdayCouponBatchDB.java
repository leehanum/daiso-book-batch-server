package com.nhnacademy.daisobatch.batch.coupon;

import com.nhnacademy.daisobatch.dto.BirthdayUserDto;
import com.nhnacademy.daisobatch.entity.coupon.CouponPolicy;
import com.nhnacademy.daisobatch.entity.coupon.UserCoupon;
import com.nhnacademy.daisobatch.listener.JobFailureNotificationListener;
import com.nhnacademy.daisobatch.listener.coupon.BirthdayChunkListener;
import com.nhnacademy.daisobatch.listener.coupon.BirthdaySkipListener;
import com.nhnacademy.daisobatch.repository.coupon.CouponPolicyJdbcRepository;
import com.nhnacademy.daisobatch.type.CouponStatus;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class BirthdayCouponBatchDB {

    private static final long BIRTHDAY_POLICY_ID = 4L;

    private final BirthdayChunkListener birthdayChunkListener;
    private final BirthdaySkipListener birthdaySkipListener;
    private final CouponPolicyJdbcRepository couponPolicyJdbcRepository;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final DataSource dataSource;

    private final BirthdayUserIdRangePartitioner birthdayUserIdRangePartitioner;

    private final JobFailureNotificationListener jobFailureNotificationListener;

    // Processor에서 재사용할 쿠폰 정책 캐시
    // (파티셔닝 들어가면 여러 스레드에서 접근 가능 → 아래에서 안전하게 초기화) 동시성 고려
    private volatile CouponPolicy birthdayPolicy;

    // 0) TaskExecutor (스레드 풀)
    @Bean(name = "birthdayTaskExecutor")
    public TaskExecutor birthdayTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("birthday-partition-");
        executor.initialize();
        return executor;
    }



    // 1) Job: ManagerStep부터 시작
    @Bean(name = "birthdayCouponJobDB")
    public Job birthdayCouponJobDB(@Qualifier("birthdayCouponManagerStepDB") Step managerStep) {
        return new JobBuilder("birthdayCouponJobDB", jobRepository)
                .start(managerStep)
                .listener(jobFailureNotificationListener)
                .build();
    }

    // 2) ManagerStep: 파티션 생성 + Worker 병렬 실행
    @Bean(name = "birthdayCouponManagerStepDB")
    public Step birthdayCouponManagerStepDB(
            @Qualifier("birthdayCouponWorkerStepDB") Step workerStep,
            @Qualifier("birthdayTaskExecutor") TaskExecutor taskExecutor // taskExecutor 멀티쓰래드
    ) {
        return new StepBuilder("birthdayCouponManagerStepDB", jobRepository)
                .partitioner("birthdayCouponWorkerStepDB", birthdayUserIdRangePartitioner)
                .step(workerStep)
                .gridSize(4) // 파티션 개수(=동시 워커 수). 필요하면 yml에서 주입해도 됨
                .taskExecutor(taskExecutor)
                .build();
    }

    // 3) WorkerStep: 각 파티션이 수행할 Chunk Step
    @Bean(name = "birthdayCouponWorkerStepDB")
    public Step birthdayCouponWorkerStepDB(
            @Qualifier("birthdayUserReaderPartitioned") JdbcPagingItemReader<BirthdayUserDto> reader,
            @Qualifier("birthdayUserProcessorDB") ItemProcessor<BirthdayUserDto, UserCoupon> processor,
            @Qualifier("birthdayUserJdbcWriterDB") JdbcBatchItemWriter<UserCoupon> writer
    ) {
        return new StepBuilder("birthdayCouponWorkerStepDB", jobRepository)
                .<BirthdayUserDto, UserCoupon>chunk(1000, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)

                .faultTolerant()
                .skip(DuplicateKeyException.class)
                .skip(IllegalArgumentException.class)
                .skip(DataIntegrityViolationException.class)
                .skipLimit(100)

                .retry(TransientDataAccessException.class)
                .retryLimit(3)

                .listener(birthdayChunkListener)
                .listener(birthdaySkipListener)
                .build();
    }

    // 4) Reader: 파티션별 ID 범위 + 월 조건으로 조회
    @Bean(name = "birthdayUserReaderPartitioned")
    @StepScope
    public JdbcPagingItemReader<BirthdayUserDto> birthdayUserReaderPartitioned(
            @Value("#{stepExecutionContext['minUserCreatedId']}") Long minUserCreatedId,
            @Value("#{stepExecutionContext['maxUserCreatedId']}") Long maxUserCreatedId,
            @Value("#{stepExecutionContext['currentMonth']}") Integer currentMonth
    ) {
        if (minUserCreatedId == null || maxUserCreatedId == null || currentMonth == null) {
            throw new IllegalStateException(
                    "Partition context missing. " +
                            "minUserCreatedId=" + minUserCreatedId +
                            ", maxUserCreatedId=" + maxUserCreatedId +
                            ", currentMonth=" + currentMonth
            );
        }

        log.info("[Reader] month={}, range={}~{}", currentMonth, minUserCreatedId, maxUserCreatedId);

        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setSelectClause("SELECT u.user_created_id AS user_created_id");

        queryProvider.setFromClause("""
            FROM Users u
            INNER JOIN Accounts a
                ON a.user_created_id = u.user_created_id
        """);

        queryProvider.setWhereClause("""
            WHERE u.birth IS NOT NULL
              AND MONTH(u.birth) = :month
              AND a.current_status_id = :statusId
              AND u.user_created_id BETWEEN :minUserCreatedId AND :maxUserCreatedId
              AND NOT EXISTS (
                  SELECT 1
                  FROM user_coupons uc
                  WHERE uc.user_created_id = u.user_created_id
                    AND uc.coupon_policy_id = :policyId
              )
        """);

        Map<String, Order> sortKeys = new LinkedHashMap<>();
        sortKeys.put("u.user_created_id", Order.ASCENDING);
        queryProvider.setSortKeys(sortKeys);

        Map<String, Object> params = new HashMap<>();
        params.put("month", currentMonth);
        params.put("statusId", 1); // ACTIVE
        params.put("policyId", BIRTHDAY_POLICY_ID);
        params.put("minUserCreatedId", minUserCreatedId);
        params.put("maxUserCreatedId", maxUserCreatedId);

        return new JdbcPagingItemReaderBuilder<BirthdayUserDto>()
                .name("birthdayUserReaderPartitioned")
                .dataSource(dataSource)
                .queryProvider(queryProvider)
                .parameterValues(params)
                .pageSize(5000)
                .fetchSize(5000)
                .rowMapper((rs, rowNum) -> new BirthdayUserDto(rs.getLong("user_created_id")))
                .build();
    }

    // 5) Processor: 정책 1회 로딩 + 엔티티 변환
    @Bean(name = "birthdayUserProcessorDB")
    @StepScope
    public ItemProcessor<BirthdayUserDto, UserCoupon> birthdayUserProcessorDB() {

        // 파티셔닝(멀티스레드) 대비: 정책 캐시를 한 번만 초기화
        CouponPolicy policy = birthdayPolicy;
        if (policy == null) {
            synchronized (this) {
                policy = birthdayPolicy;
                if (policy == null) {
                    birthdayPolicy = policy = couponPolicyJdbcRepository.findById(BIRTHDAY_POLICY_ID);
                }
            }
        }

        final CouponPolicy finalPolicy = policy;

        return item -> {
            Long userId = item.getUserCreatedId();

            LocalDate now = LocalDate.now();
            LocalDateTime issuedAt = LocalDateTime.now();
            LocalDateTime expiryAt = now.withDayOfMonth(now.lengthOfMonth()).atTime(23, 59, 59);

            return UserCoupon.builder()
                    .couponPolicy(finalPolicy)
                    .userId(userId)
                    .status(CouponStatus.ISSUED)
                    .issuedAt(issuedAt)
                    .expiryAt(expiryAt)
                    .usedAt(null)
                    .build();
        };
    }

    // 6) Writer: INSERT IGNORE로 중복 방지
    @Bean(name = "birthdayUserJdbcWriterDB")
    public JdbcBatchItemWriter<UserCoupon> birthdayUserJdbcWriterDB() {
        return new JdbcBatchItemWriterBuilder<UserCoupon>()
                .dataSource(dataSource)
                .sql("""
                    INSERT IGNORE INTO user_coupons
                      (user_created_id, coupon_policy_id, status, issue_at, expiry_at, used_at)
                    VALUES
                      (:userId, :couponPolicyId, :status, :issuedAt, :expiryAt, :usedAt)
                """)
                .itemSqlParameterSourceProvider(item -> {
                    MapSqlParameterSource ps = new MapSqlParameterSource();
                    ps.addValue("userId", item.getUserId());
                    ps.addValue("couponPolicyId", item.getCouponPolicy().getCouponPolicyId());
                    ps.addValue("status", item.getStatus().name());
                    ps.addValue("issuedAt", item.getIssuedAt());
                    ps.addValue("expiryAt", item.getExpiryAt());
                    ps.addValue("usedAt", item.getUsedAt());
                    return ps;
                })
                .build();
    }
}