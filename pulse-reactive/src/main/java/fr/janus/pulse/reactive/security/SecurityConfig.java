package fr.janus.pulse.reactive.security;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.core.userdetails.MapReactiveUserDetailsService;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.web.server.SecurityWebFilterChain;

/**
 * Sécurité <strong>réactive</strong> de {@code pulse-reactive} (lab J4-2 A).
 *
 * <p>{@code @EnableWebFluxSecurity} active l'infrastructure de sécurité WebFlux ; déclarer notre
 * propre {@link SecurityWebFilterChain} <em>remplace</em> la chaîne par défaut de Boot (qui
 * sécuriserait tout avec un mot de passe généré au démarrage). La chaîne est exprimée en
 * <strong>DSL lambda</strong> ({@link ServerHttpSecurity}), le style à l'état de l'art.
 *
 * <p>Posture retenue pour le lab :
 * <ul>
 *   <li>{@code /actuator/**} ouvert : la santé et le scrape Prometheus doivent rester
 *       accessibles au monitoring (le différentiel de charge interroge aussi ces endpoints).</li>
 *   <li>tout le reste de {@code /api/**} exige une authentification.</li>
 *   <li><strong>HTTP Basic</strong> (le brief autorise « JWT resource server OU basic pour le
 *       lab ») : stateless, suffisant pour démontrer la traversée du {@code SecurityContext}.</li>
 *   <li><strong>CSRF désactivé</strong> : l'API est sans session (pas de cookie d'auth), la
 *       protection CSRF — active par défaut sur {@link ServerHttpSecurity} — bloquerait sinon
 *       les {@code POST} avec un 403. Sur une API stateless à authentification par en-tête,
 *       la désactiver est le réglage correct, pas un contournement.</li>
 *   <li>pas de {@code formLogin} : une API ne redirige pas vers une page de login.</li>
 * </ul>
 *
 * <p>Le principal authentifié ici est lu plus loin dans le pipeline via
 * {@code ReactiveSecurityContextHolder} (cf. {@code AlertService}) — ce qui prouve que le
 * {@code SecurityContext} traverse les changements de thread du pipeline réactif (lien J3-1),
 * sans jamais passer par un {@code ThreadLocal} nu.
 */
@Configuration(proxyBeanMethods = false)
@EnableWebFluxSecurity
public class SecurityConfig {

    @Bean
    public SecurityWebFilterChain securityWebFilterChain(ServerHttpSecurity http) {
        return http
                .authorizeExchange(exchange -> exchange
                        // Monitoring ouvert : health + scrape Prometheus (cf. management.endpoints).
                        .pathMatchers("/actuator/**").permitAll()
                        // Le CRUD et le reste de l'API exigent un utilisateur authentifié.
                        .pathMatchers("/api/**").authenticated()
                        .anyExchange().authenticated())
                .httpBasic(org.springframework.security.config.Customizer.withDefaults())
                // API stateless à auth par en-tête : pas de CSRF (sinon 403 sur POST).
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .formLogin(ServerHttpSecurity.FormLoginSpec::disable)
                .build();
    }

    /**
     * Utilisateurs en mémoire pour le lab ({@link MapReactiveUserDetailsService} — non bloquant,
     * pas d'accès base sur la chaîne réactive : l'anti-pattern « UserDetailsService bloquant »
     * est ainsi évité par construction). Mots de passe en clair via le préfixe {@code {noop}}
     * reconnu par l'encodeur délégant par défaut — acceptable pour un support de formation.
     */
    @Bean
    public MapReactiveUserDetailsService userDetailsService() {
        UserDetails user = User.withUsername("user").password("{noop}password").roles("USER").build();
        UserDetails alice = User.withUsername("alice").password("{noop}password").roles("USER").build();
        UserDetails bob = User.withUsername("bob").password("{noop}password").roles("USER").build();
        return new MapReactiveUserDetailsService(user, alice, bob);
    }
}
