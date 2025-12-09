from django.db import models
from django.utils import timezone
from datetime import date


class DemandData(models.Model):
    # Existing fields
    current_demand = models.CharField(
        max_length=50,
        help_text="Current demand value in MW"
    )
    yesterday_demand = models.CharField(
        max_length=50,
        help_text="Yesterday's demand value in MW"
    )

    # NEW: Field for the time block string, e.g., "10:15 - 10:30"
    time_block = models.CharField(
        max_length=50,
        help_text="The time block string extracted from the site",
        null=True,
        blank=True
    )

    # NEW: Field for just the date from the site
    date = models.DateField(
        help_text="The date extracted from the website",
        null=True,
        blank=True
    )

    # Timestamp for when the script ran
    captured_at = models.DateTimeField(
        default=timezone.now,
        help_text="Timestamp of when the data was captured"
    )

    def __str__(self):
        local_time = timezone.localtime(self.captured_at)
        return f"Data captured at {local_time.strftime('%Y-%m-%d %H:%M:%S %Z')}"

    class Meta:
        ordering = ['-captured_at']


class Nrldc2AData(models.Model):
    objects = None
    objcts = None
    report_date = models.DateField()
    state = models.CharField(max_length=100, null=True, blank=True)

    thermal = models.FloatField(null=True, blank=True)
    hydro = models.FloatField(null=True, blank=True)
    gas_naptha_diesel = models.FloatField(null=True, blank=True)
    solar = models.FloatField(null=True, blank=True)
    wind = models.FloatField(null=True, blank=True)
    other_biomass = models.FloatField(null=True, blank=True)
    total = models.FloatField(null=True, blank=True)
    drawal_sch = models.FloatField(null=True, blank=True)
    act_drawal = models.FloatField(null=True, blank=True)
    ui = models.FloatField(null=True, blank=True)
    requirement = models.FloatField(null=True, blank=True)
    shortage = models.FloatField(null=True, blank=True)
    consumption = models.FloatField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Table 2A Data for {self.report_date} - {self.state}"

    class Meta:
        verbose_name = "Table 2A Data"
        verbose_name_plural = "Table 2A Data"
        unique_together = ('report_date', 'state')


class Nrldc2CData(models.Model):
    objects = None
    report_date = models.DateField(default=date.today)
    state = models.CharField(max_length=100, null=True, blank=True)

    max_demand = models.FloatField(null=True, blank=True)
    time_max = models.CharField(max_length=50, null=True, blank=True)
    shortage_during = models.FloatField(null=True, blank=True)
    req_max_demand = models.FloatField(null=True, blank=True)

    max_req_day = models.FloatField(null=True, blank=True)
    time_max_req = models.CharField(max_length=50, null=True, blank=True)
    shortage_max_req = models.FloatField(null=True, blank=True)
    demand_met_max_req = models.FloatField(null=True, blank=True)

    min_demand_met = models.FloatField(null=True, blank=True)
    time_min_demand = models.CharField(max_length=50, null=True, blank=True)

    ace_max = models.FloatField(null=True, blank=True)
    ace_min = models.FloatField(null=True, blank=True)
    time_ace_max = models.CharField(max_length=50, null=True, blank=True)
    time_ace_min = models.CharField(max_length=50, null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Table 2C Data for {self.report_date} - {self.state}"

    class Meta:
        verbose_name = "Table 2C Data"
        verbose_name_plural = "Table 2C Data"
        unique_together = ('report_date', 'state')



class PosocoTableA(models.Model):
    """Table A - Demand, Energy, Hydro, Wind, etc. by Region"""
    category = models.CharField(max_length=255)
    nr = models.CharField(max_length=50, null=True, blank=True)
    wr = models.CharField(max_length=50, null=True, blank=True)
    sr = models.CharField(max_length=50, null=True, blank=True)
    er = models.CharField(max_length=50, null=True, blank=True)
    ner = models.CharField(max_length=50, null=True, blank=True)
    total = models.CharField(max_length=50, null=True, blank=True)
    report_date = models.DateField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'posoco_posocotablea'  # 👈 Add this line to specify the exact table name

    def __str__(self):
        return f"TableA | {self.category} | {self.report_date}"


class PosocoTableG(models.Model):
    """Table G - Generation mix by fuel type"""
    fuel_type = models.CharField(max_length=255)
    nr = models.CharField(max_length=50, null=True, blank=True)
    wr = models.CharField(max_length=50, null=True, blank=True)
    sr = models.CharField(max_length=50, null=True, blank=True)
    er = models.CharField(max_length=50, null=True, blank=True)
    ner = models.CharField(max_length=50, null=True, blank=True)
    all_india = models.CharField(max_length=50, null=True, blank=True)
    share_percent = models.CharField(max_length=50, null=True, blank=True)
    report_date = models.DateField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'posoco_posocotableg'  # 👈 Add this line for the second table as well

    def __str__(self):
        return f"TableG | {self.fuel_type} | {self.report_date}"

from datetime import date

class Srldc2AData(models.Model):
    objects = []
    report_date = models.DateField()  # Multiple states per day allowed
    state = models.CharField(max_length=100, null=True, blank=True)

    thermal = models.FloatField(null=True, blank=True)
    hydro = models.FloatField(null=True, blank=True)
    gas_naptha_diesel = models.FloatField(null=True, blank=True)
    solar = models.FloatField(null=True, blank=True)
    wind = models.FloatField(null=True, blank=True)
    others = models.FloatField(null=True, blank=True)
    # total = models.FloatField(null=True, blank=True)
    net_sch = models.FloatField(null=True, blank=True)
    drawal = models.FloatField(null=True, blank=True)
    ui = models.FloatField(null=True, blank=True)
    availability = models.FloatField(null=True, blank=True)
    demand_met = models.FloatField(null=True, blank=True)

    shortage = models.FloatField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Table 2A Data for {self.report_date} - {self.state}"

    class Meta:
        verbose_name = "Table 2A Data"
        verbose_name_plural = "Table 2A Data"
        unique_together = ('report_date', 'state')


class Srldc2CData(models.Model):
    objects = None
    report_date = models.DateField(default=date.today)
    state = models.CharField(max_length=100, null=True, blank=True)

    max_demand = models.FloatField(null=True, blank=True)
    time = models.CharField(max_length=50, null=True, blank=True)
    shortage_max_demand = models.FloatField(null=True, blank=True)
    req_max_demand = models.FloatField(null=True, blank=True)

    demand_max_req = models.FloatField(null=True, blank=True)
    time_max_req = models.CharField(max_length=50, null=True, blank=True)
    shortage_max_req = models.FloatField(null=True, blank=True)
    max_req_day = models.FloatField(null=True, blank=True)
    ace_max = models.FloatField(null=True, blank=True)
    time_ace_max = models.CharField(max_length=50, null=True, blank=True)
    ace_min = models.FloatField(null=True, blank=True)
    time_ace_min = models.CharField(max_length=50, null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Table 2C Data for {self.report_date} - {self.state}"

    class Meta:
        verbose_name = "Table 2C Data"
        verbose_name_plural = "Table 2C Data"
        unique_together = ('report_date', 'state')


class Wrldc2AData(models.Model):
    objects = None
    report_date = models.DateField()  # Multiple states per day allowed
    state = models.CharField(max_length=100, null=True, blank=True)

    thermal = models.CharField(max_length=50, null=True, blank=True)
    hydro = models.CharField(max_length=50, null=True, blank=True)
    gas = models.CharField(max_length=50, null=True, blank=True)
    solar = models.CharField(max_length=50, null=True, blank=True)
    wind = models.CharField(max_length=50, null=True, blank=True)
    others = models.CharField(max_length=50, null=True, blank=True)
    total = models.CharField(max_length=50, null=True, blank=True)
    net_sch = models.CharField(max_length=50, null=True, blank=True)
    drawal = models.CharField(max_length=50, null=True, blank=True)
    ui = models.CharField(max_length=50, null=True, blank=True)
    availability = models.CharField(max_length=50, null=True, blank=True)
    consumption = models.CharField(max_length=50, null=True, blank=True)
    shortage = models.CharField(max_length=50, null=True, blank=True)
    requirement = models.CharField(max_length=50, null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Table 2A Data for {self.report_date} - {self.state}"

    class Meta:
        verbose_name = "Table 2A Data"
        verbose_name_plural = "Table 2A Data"
        unique_together = ('report_date', 'state')


class Wrldc2CData(models.Model):
    objects = None
    report_date = models.DateField(default=date.today)
    state = models.CharField(max_length=100, null=True, blank=True)

    max_demand_day = models.FloatField(null=True, blank=True)
    time = models.CharField(max_length=50, null=True, blank=True)
    shortage_max_demand = models.CharField(max_length=50, null=True, blank=True)
    req_max_demand = models.CharField(max_length=50, null=True, blank=True)
    ace_max = models.CharField(max_length=50, null=True, blank=True)
    time_ace_max = models.CharField(max_length=50, null=True, blank=True)
    ace_min = models.CharField(max_length=50, null=True, blank=True)
    time_ace_min = models.CharField(max_length=50, null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)


    def __str__(self):
        return f"Table 2C Data for {self.report_date} - {self.state}"

    class Meta:
        verbose_name = "Table 2C Data"
        verbose_name_plural = "Table 2C Data"
        unique_together = ('report_date', 'state')


class SRLDC3BData(models.Model):
    report_date = models.DateField(null=True, blank=True)
    reporting_datetime = models.DateTimeField(null=True, blank=True)

    station = models.TextField()

    installed_capacity_mw = models.IntegerField(null=True, blank=True)
    peak_1900_mw = models.IntegerField(null=True, blank=True)
    offpeak_0300_mw = models.IntegerField(null=True, blank=True)
    day_peak_mw = models.IntegerField(null=True, blank=True)
    day_peak_hrs = models.CharField(max_length=64, null=True, blank=True)

    # OLD FORMAT → stores remapped values here
    day_energy_mu = models.DecimalField(max_digits=14, decimal_places=4, null=True, blank=True)
    avg_mw = models.DecimalField(max_digits=14, decimal_places=4, null=True, blank=True)

    # NEW FORMAT STILL NEEDS THESE
    min_generation_mw = models.DecimalField(max_digits=12, decimal_places=3, null=True, blank=True)
    min_generation_hrs = models.CharField(max_length=64, null=True, blank=True)

    gross_energy_mu = models.DecimalField(max_digits=14, decimal_places=4, null=True, blank=True)
    net_energy_mu = models.DecimalField(max_digits=14, decimal_places=4, null=True, blank=True)

    row_type = models.CharField(max_length=32, null=True, blank=True)
    source_page = models.IntegerField(null=True, blank=True)
    source_table_index = models.IntegerField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ('report_date', 'station')

    def __str__(self):
        return f"{self.station} - {self.report_date}"
