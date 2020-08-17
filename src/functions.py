import pandas as pd


def fitting_curve(x):
    T_ave_fit
    DT_y_fit
    T = len(h)
    omega = 2 * pi / T
    phi
    return T_ave_fit + DT_y_fit * np.cos(x * omega - phi)

def ground_temperature_day(t):
    # t corresponds to the number of day from 1 to 365
    T_banks = Tg_und + DT_y * exp(-zz * sqrt(pi / (alpha * t_0))) * cos(
        2 * pi / t_0 * (t - dd_max) - zz * sqrt(pi / (alpha * t_0)))  # Banks + t_shift

    T_kusuda = Tg_und - DT_y * exp(-zz * sqrt(pi / (alpha * t_0))) * cos(
        2 * pi / t_0 * (t - dd_min - zz / 2 * sqrt(t_0 / (pi * alpha))))  # Kusuda

    # return  (pd.Series({'T_banks': T_banks,'T_kusuda': T_kusuda,'T_banks_noshift:': T_banks_noshift}))
    return (pd.Series({'T_banks': T_banks, 'T_kusuda': T_kusuda}))


def ground_temperature_month(month):
    # t corresponds to the number of month from 1 to 12
    T_banks = Tg_und + DT_y * exp(-zz * sqrt(pi / (alpha * t_0))) * cos(
        2 * pi / t_0 * (15 + (month - 1) * 30 - dd_max) - zz * sqrt(pi / (alpha * t_0)))  # Banks + t_shift

    T_kusuda = Tg_und - DT_y * exp(-zz * sqrt(pi / (alpha * t_0))) * cos(
        2 * pi / t_0 * (15 + (month - 1) * 30 - dd_min - zz / 2 * sqrt(t_0 / (pi * alpha))))  # Kusuda ->Selva's
    return (pd.Series({'T_banks': T_banks, 'T_kusuda': T_kusuda}))


def ground_temperature_hour(t):
    # t is time in hours, but the calculation is done is seconds
    T_banks = Tg_und + DT_y * exp(-zz * sqrt(pi / (alpha_sec * t_sec))) * cos(
        2 * pi / t_sec * (t - dd_max * 24) * 3600 - zz * sqrt(pi / (alpha_sec * t_sec)))  # Banks + t_shift ->Marco's

    T_kusuda = Tg_und - DT_y * exp(-zz * sqrt(pi / (alpha_sec * t_sec))) * cos(
        2 * pi / t_sec * ((t - dd_min * 24) * 3600 - zz / 2 * sqrt(t_sec / (pi * alpha_sec))))  # Kusuda
    return (pd.Series({'T_banks': T_banks, 'T_kusuda': T_kusuda}))
