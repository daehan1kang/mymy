import os
import jax
import jax.numpy as jnp
import lineax as lx
from jax import jit
from jaxtyping import Array, Float, Scalar
from scipy.stats import chi2
from scipy.stats import t as t_dist

@jit
def solve_ols_with_lineax(
    X: Float[Array, "n p"], y: Float[Array, "n"]
) -> tuple[Float[Array, "p"], Float[Scalar, ""]]:
    """
    Estimates OLS parameters using Lineax for numerical stability.

    Args:
        X: Design matrix of shape (n, p).
        y: Target vector of shape (n,).

    Returns:
        beta: Estimated regression coefficients of shape (p,).
        sigma_sq: Unbiased estimator of the error variance (scalar).
    """
    n, p = X.shape

    # 1. Estimate Beta: Using QR decomposition-based solver in Lineax
    # Solves the least squares problem: X * beta = y
    operator = lx.MatrixLinearOperator(X)
    solver = lx.QR()
    solution = lx.linear_solve(operator, y, solver=solver)
    beta = solution.value

    # 2. Estimate Sigma^2 (Unbiased Estimator)
    # Computed as RSS / (n - p)
    y_hat = X @ beta
    rss = jnp.sum((y - y_hat) ** 2)
    sigma_sq = rss / (n - p)

    return beta, sigma_sq


def get_ols_statistics(
    X: Float[Array, "n p"],
    y: Float[Array, "n"],
    beta: Float[Array, "p"],
    sigma_sq: Float[Array, ""],
):
    """
    Calculates overall model goodness-of-fit statistics.
    """
    n, p = X.shape
    # Convert unbiased sigma_sq to MLE sigma_sq for log-likelihood calculation
    sigma_sq_mle = sigma_sq * (n - p) / n
    y_hat = X @ beta
    res = y - y_hat
    rss = jnp.sum(res**2)

    # 1. R-squared (Coefficient of Determination)
    # TSS: Total Sum of Squares (y - y_bar)
    tss = jnp.sum((y - jnp.mean(y)) ** 2)
    r2 = 1 - (rss / tss)

    # 2. Adj. R-squared
    # Adjusts R2 based on degrees of freedom to penalize complexity
    adj_r2 = 1 - (1 - r2) * (n - 1) / (n - p)

    # 3. F-statistic (Overall Significance)
    # H0: All non-intercept coefficients are zero
    msm = (tss - rss) / (p - 1)  # Mean Square Model
    mse = rss / (n - p)          # Mean Square Error
    f_stat = msm / mse

    # 4. Log-Likelihood
    # MLE formula assuming normally distributed errors
    log_like = -n / 2 * jnp.log(2 * jnp.pi * sigma_sq_mle) - rss / (2 * sigma_sq_mle)

    # 5. AIC & BIC (Information Criteria)
    # k follows statsmodels OLS convention (number of regressors)
    k = p
    aic = 2 * k - 2 * log_like
    bic = k * jnp.log(n) - 2 * log_like

    return {
        "R2": r2,
        "Adj_R2": adj_r2,
        "F_stat": f_stat,
        "Log_Like": log_like,
        "AIC": aic,
        "BIC": bic,
    }


def get_coefficient_statistics(
    X: Float[Array, "n p"],
    beta: Float[Array, "p"],
    sigma_sq: Float[Array, ""],
):
    """
    Computes statistical significance metrics for individual coefficients.
    
    Note: sigma_sq must be the unbiased estimator (RSS / (n-p)).
    """
    n, p = X.shape

    # 1. Covariance Matrix: sigma^2 * (X'X)^-1
    xtx = X.T @ X
    xtx_inv = jnp.linalg.inv(xtx)
    cov_beta = sigma_sq * xtx_inv

    # 2. Standard Error (SE): Square root of the diagonal elements of Cov matrix
    se = jnp.sqrt(jnp.diag(cov_beta))

    # 3. t-statistic: Ratio of estimate to its standard error
    t_stats = beta / se

    # 4. p-value: Two-tailed test using the t-distribution
    # Degrees of freedom: n - p
    p_values = 2 * (1 - t_dist.cdf(jnp.abs(t_stats), df=n - p))

    # 5. 95% Confidence Intervals
    t_crit = t_dist.ppf(0.975, df=n - p)
    ci_lower = beta - t_crit * se
    ci_upper = beta + t_crit * se

    return {
        "SE": se,
        "t_stats": t_stats,
        "p_values": p_values,
        "ci_lower": ci_lower,
        "ci_upper": ci_upper,
    }


def get_residual_diagnostics(
    X: Float[Array, "n p"], y: Float[Array, "n"], beta: Float[Array, "p"]
):
    """
    Performs diagnostic tests on residuals to check OLS assumptions.
    """
    n, p = X.shape
    e = y - X @ beta
    mu2 = jnp.mean(e**2)

    # 1. Skewness & Kurtosis
    # Measures the symmetry and peakedness of residual distribution
    skew = jnp.mean(e**3) / (mu2**1.5)
    kurt = jnp.mean(e**4) / (mu2**2)

    # 2. Jarque-Bera Test (Normality Test)
    # H0: Residuals are normally distributed
    jb_stat = (n / 6.0) * (skew**2 + (1.0 / 4.0) * (kurt - 3.0) ** 2)
    jb_p = 1.0 - chi2.cdf(jb_stat, df=2)

    # 3. Durbin-Watson (Autocorrelation Test)
    # Values near 2 imply no autocorrelation.
    # Near 0: Positive autocorrelation; Near 4: Negative autocorrelation.
    diff_resids = jnp.diff(e)
    dw_stat = jnp.sum(diff_resids**2) / jnp.sum(e**2)

    # 4. Condition Number (Multicollinearity Diagnostic)
    # Computed using Singular Value Decomposition for numerical stability.
    # Values > 30 suggest potential multicollinearity issues.
    s_vals = jnp.linalg.svd(X, compute_uv=False)
    cond_no = jnp.max(s_vals) / jnp.min(s_vals)

    return {
        "Skew": skew,
        "Kurtosis": kurt,
        "JB_stat": jb_stat,
        "JB_p": jb_p,
        "DW_stat": dw_stat,
        "Cond_No": cond_no,
    }

from typing import Any, Dict

def run_regression_analysis(
    X: Float[Array, "n p"], 
    y: Float[Array, "n"]
) -> Dict[str, Any]:
    """
    A comprehensive wrapper that executes the full OLS estimation 
    and returns all diagnostic and inferential statistics.

    Args:
        X: Design matrix (n, p).
        y: Target vector (n,).

    Returns:
        A dictionary containing:
        - estimates: Beta coefficients and sigma squared.
        - model_stats: R2, Adj-R2, F-stat, Log-Likelihood, AIC, BIC.
        - coef_stats: SE, t-stats, p-values, and Confidence Intervals.
        - diagnostics: Skew, Kurtosis, JB, DW, and Condition Number.
    """
    # 1. Parameter Estimation
    beta, sigma_sq = solve_ols_with_lineax(X, y)
    
    # 2. Overall Model Statistics
    model_stats = get_ols_statistics(X, y, beta, sigma_sq)
    
    # 3. Coefficient-level Statistics (Inference)
    coef_stats = get_coefficient_statistics(X, beta, sigma_sq)
    
    # 4. Residual Diagnostics
    diagnostics = get_residual_diagnostics(X, y, beta)
    
    # Consolidate all results into a single dictionary
    results = {
        "beta": beta,
        "sigma_sq": sigma_sq,
        **model_stats,
        **coef_stats,
        **diagnostics
    }
    
    return results

# Example Usage:
# results = run_regression_analysis(X_data, y_data)
# print(f"R-squared: {results['R2']:.4f}")
# print(f"AIC: {results['AIC']:.2f}")

import cvxpy as cp
import jax.numpy as jnp
from cvxpylayers.jax import CvxpyLayer
from jaxtyping import Array, Float, Scalar

def solve_ols_with_lineax(
    X: Float[Array, "n p"], y: Float[Array, "n"]
) -> tuple[Float[Array, "p"], Float[Scalar, ""]]:
    """
    Estimates OLS parameters using cvxpylayers.
    Matches the signature and return types of the original Lineax-based function.
    """
    n, p = X.shape

    # 1. Define the CVXPY problem
    beta_param = cp.Variable(p)
    X_param = cp.Parameter((n, p))
    y_param = cp.Parameter(n)
    
    # OLS Objective: Minimize the sum of squared residuals
    objective = cp.Minimize(cp.sum_squares(X_param @ beta_param - y_param))
    problem = cp.Problem(objective)
    
    # 2. Create the Differentiable Layer
    # This allows the optimization problem to be integrated into JAX's autodiff
    cvxpy_layer = CvxpyLayer(problem, parameters=[X_param, y_param], variables=[beta_param])
    
    # 3. Solve the problem
    # The output of cvxpy_layer is a list of optimal variables
    solution_list = cvxpy_layer(X, y)
    beta = solution_list[0]

    # 4. Estimate Sigma^2 (Unbiased Estimator)
    y_hat = X @ beta
    rss = jnp.sum((y - y_hat) ** 2)
    sigma_sq = rss / (n - p)

    return beta, sigma_sq
