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
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

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

    // Processor에서 재사용할 쿠폰 쟁책 캐시
    private CouponPolicy birthdayPolicy;

    private final JobFailureNotificationListener jobFailureNotificationListener;

    // 1. Job 정의
    @Bean(name = "birthdayCouponJobDB")
    public Job birthdayCouponJobDB(@Qualifier("birthdayCouponStepDB") Step step) {
        return new JobBuilder("birthdayCouponJobDB", jobRepository)
                .start(step)
                .listener(jobFailureNotificationListener)
                .build();
    }

    // 2. Step 정의
    @Bean(name = "birthdayCouponStepDB")
    public Step birthdayCouponStepDB(
            @Qualifier("birthdayUserReaderDB") JdbcPagingItemReader<BirthdayUserDto> reader,
            @Qualifier("birthdayUserProcessorDB") ItemProcessor<BirthdayUserDto, UserCoupon> processor,
            @Qualifier("birthdayUserJdbcWriterDB") JdbcBatchItemWriter<UserCoupon> writer
    ) {
        return new StepBuilder("birthdayCouponStepDB", jobRepository)
                .<BirthdayUserDto, UserCoupon>chunk(1000, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)

                // 실패 처리 추가
                .faultTolerant()
                // 중복 발급(유니크 키) / 무결성 문제는 "데이터 문제"로 보고 스킵
                .skip(DuplicateKeyException.class) // 이미 쿠폰이 있어 PK 충돌이 나면 Skip
                .skip(IllegalArgumentException.class) // 로직상 데이터 문제 시 Skip
                .skip(DataIntegrityViolationException.class)
                .skipLimit(100)

                // Retry 전략: DB 연결 문제나 데드락은 재시도
                .retry(TransientDataAccessException.class) // 일시적 DB 오류
                .retryLimit(3)

                .listener(birthdayChunkListener) // 생일 쿠폰 배치에서 chunk 단위로 성공/실패/진행 상황을 알려주는 로그 감시자
                .listener(birthdaySkipListener)
                .build();
    }

    // 3. Reader (DB 직접 조회 + 페이징)
    @Bean(name = "birthdayUserReaderDB")
    @StepScope
    public JdbcPagingItemReader<BirthdayUserDto> birthdayUserReaderDB(
            @Value("#{jobParameters['currentMonth']}") Integer currentMonth
    ) {
        int month = (currentMonth != null)
                ? currentMonth
                : LocalDate.now().getMonthValue();

        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setSelectClause("SELECT DISTINCT u.user_created_id AS user_created_id");

        queryProvider.setFromClause("""
            FROM Users u
            INNER JOIN Accounts a
                ON a.user_created_id = u.user_created_id
        """);
        queryProvider.setWhereClause("""
            WHERE u.birth IS NOT NULL
              AND MONTH(u.birth) = :month
              AND a.current_status_id = :statusId
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
        params.put("month", month);
        params.put("statusId", 1); // ACTIVE
        params.put("policyId", BIRTHDAY_POLICY_ID);

        return new JdbcPagingItemReaderBuilder<BirthdayUserDto>()
                .name("birthdayUserReaderDB")
                .dataSource(dataSource)
                .queryProvider(queryProvider)
                .parameterValues(params)
                .pageSize(5000)
                .fetchSize(5000)
                .rowMapper((rs, rowNum) ->
                        new BirthdayUserDto(rs.getLong("user_created_id"))
                )
                .build();
    }

    // 4. Processor: DTO -> Entity 변환 (중복 발급 장지 + 정책 캐싱)
    @Bean(name = "birthdayUserProcessorDB")
    @StepScope
    public ItemProcessor<BirthdayUserDto, UserCoupon> birthdayUserProcessorDB() {
        if (birthdayPolicy == null) {
            birthdayPolicy = couponPolicyJdbcRepository.findById(BIRTHDAY_POLICY_ID);
        }

        return item -> {
            Long userId = item.getUserCreatedId();

            LocalDate now = LocalDate.now();
            LocalDateTime issuedAt = LocalDateTime.now();
            LocalDateTime expiryAt = now.withDayOfMonth(now.lengthOfMonth()).atTime(23, 59, 59);

            return UserCoupon.builder()
                    .couponPolicy(birthdayPolicy)
                    .userId(userId)
                    .status(CouponStatus.ISSUED)
                    .issuedAt(issuedAt)
                    .expiryAt(expiryAt)
                    .usedAt(null)
                    .build();
        };
    }

    // 5. Writer: DB에 저장 (null 제거 후 저장)
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
