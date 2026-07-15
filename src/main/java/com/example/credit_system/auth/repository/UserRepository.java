package com.example.credit_system.auth.repository;

import com.example.credit_system.auth.domain.User;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface UserRepository extends JpaRepository<User, Long> {

    /** 사용자 이름으로 사용자를 조회한다. */
    Optional<User> findByUsername(String username);
}
