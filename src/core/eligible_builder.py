class EligibleBuilder:
    """
    Builder class for filtering eligible taxpayers based on risk indicators.
    Each ELIGIBLE_IND method filters data based on:
    - ANNEE_FISCAL > threshold year (default 2021)
    - RISQUE_IND_15_A == 'rouge'
    - RISQUE_IND_15_B == 'rouge'
    - The specific RISQUE_IND_X in the specified risk levels
    """
    
    DEFAULT_RISK_LEVELS = ['rouge', 'orange', 'jaune']
    DEFAULT_YEAR_THRESHOLD = 2021
    
    # List of all indicator numbers supported
    SUPPORTED_INDICATORS = [1, 2, 4, 5, 6, 8, 9, 10, 12, 13, 14, 16, 20, 21, 23, 24, 25, 26, 27, 29, 30, 32, 33]
    
    def __init__(self, data):
        self.data = data

    def _get_eligible(self, indicator_num: int, risk_levels=None, year_threshold=None):
        """
        Generic method to filter eligible taxpayers for a given indicator.
        
        Args:
            indicator_num: The risk indicator number (e.g., 1 for RISQUE_IND_1)
            risk_levels: List of risk levels to include (default: ['rouge', 'orange', 'jaune'])
            year_threshold: Minimum fiscal year (exclusive, default: 2021)
            
        Returns:
            DataFrame of eligible taxpayers
        """
        if risk_levels is None:
            risk_levels = self.DEFAULT_RISK_LEVELS
        if year_threshold is None:
            year_threshold = self.DEFAULT_YEAR_THRESHOLD
            
        risk_column = f'RISQUE_IND_{indicator_num}'
        
        # Verify column exists
        if risk_column not in self.data.columns:
            raise ValueError(f"Column '{risk_column}' not found in data")
        
        eligible = self.data[
            (self.data['ANNEE_FISCAL'] > year_threshold) &
            (self.data['RISQUE_IND_15_B'] == 'rouge') &
            (self.data['RISQUE_IND_15_A'] == 'rouge') &
            (self.data[risk_column].isin(risk_levels))
        ]
        
        return eligible

    def get_eligible_by_indicator(self, indicator_num: int, risk_levels=None, year_threshold=None):
        """
        Public method to get eligible taxpayers for any supported indicator.
        
        Args:
            indicator_num: The indicator number (1, 2, 4, 5, 8, 9, 10, 12, 13, 14, 16, 20, 21, 23, 24, 25, 26, 27, 29, 30, 32, 33)
            risk_levels: List of risk levels to include
            year_threshold: Minimum fiscal year (exclusive)
            
        Returns:
            DataFrame of eligible taxpayers
        """
        if indicator_num not in self.SUPPORTED_INDICATORS:
            raise ValueError(f"Indicator {indicator_num} not supported. Supported: {self.SUPPORTED_INDICATORS}")
        return self._get_eligible(indicator_num, risk_levels, year_threshold)

    # Individual methods for backward compatibility and explicit usage
    def ELIGIBLE_IND1(self, risk_levels=None, year_threshold=None):
        return self._get_eligible(1, risk_levels, year_threshold)
    
    def ELIGIBLE_IND2(self, risk_levels=None, year_threshold=None):
        return self._get_eligible(2, risk_levels, year_threshold)
    
    def ELIGIBLE_IND4(self, risk_levels=None, year_threshold=None):
        return self._get_eligible(4, risk_levels, year_threshold)
    
    def ELIGIBLE_IND5(self, risk_levels=None, year_threshold=None):
        return self._get_eligible(5, risk_levels, year_threshold)
    
    def ELIGIBLE_IND6(self, risk_levels=None, year_threshold=None):
        return self._get_eligible(6, risk_levels, year_threshold)
    
    def ELIGIBLE_IND8(self, risk_levels=None, year_threshold=None):
        return self._get_eligible(8, risk_levels, year_threshold)
    
    def ELIGIBLE_IND9(self, risk_levels=None, year_threshold=None):
        return self._get_eligible(9, risk_levels, year_threshold)
    
    def ELIGIBLE_IND10(self, risk_levels=None, year_threshold=None):
        return self._get_eligible(10, risk_levels, year_threshold)
    
    def ELIGIBLE_IND12(self, risk_levels=None, year_threshold=None):
        return self._get_eligible(12, risk_levels, year_threshold)
    
    def ELIGIBLE_IND13(self, risk_levels=None, year_threshold=None):
        return self._get_eligible(13, risk_levels, year_threshold)
    
    def ELIGIBLE_IND14(self, risk_levels=None, year_threshold=None):
        return self._get_eligible(14, risk_levels, year_threshold)
    
    def ELIGIBLE_IND16(self, risk_levels=None, year_threshold=None):
        return self._get_eligible(16, risk_levels, year_threshold)
    
    def ELIGIBLE_IND20(self, risk_levels=None, year_threshold=None):
        return self._get_eligible(20, risk_levels, year_threshold)
    
    def ELIGIBLE_IND21(self, risk_levels=None, year_threshold=None):
        return self._get_eligible(21, risk_levels, year_threshold)
    
    def ELIGIBLE_IND23(self, risk_levels=None, year_threshold=None):
        return self._get_eligible(23, risk_levels, year_threshold)
    
    def ELIGIBLE_IND24(self, risk_levels=None, year_threshold=None):
        return self._get_eligible(24, risk_levels, year_threshold)
    
    def ELIGIBLE_IND25(self, risk_levels=None, year_threshold=None):
        return self._get_eligible(25, risk_levels, year_threshold)
    
    def ELIGIBLE_IND26(self, risk_levels=None, year_threshold=None):
        return self._get_eligible(26, risk_levels, year_threshold)
    
    def ELIGIBLE_IND27(self, risk_levels=None, year_threshold=None):
        return self._get_eligible(27, risk_levels, year_threshold)
    
    def ELIGIBLE_IND29(self, risk_levels=None, year_threshold=None):
        return self._get_eligible(29, risk_levels, year_threshold)
    
    def ELIGIBLE_IND30(self, risk_levels=None, year_threshold=None):
        return self._get_eligible(30, risk_levels, year_threshold)
    
    def ELIGIBLE_IND32(self, risk_levels=None, year_threshold=None):
        return self._get_eligible(32, risk_levels, year_threshold)
    
    def ELIGIBLE_IND33(self, risk_levels=None, year_threshold=None):
        return self._get_eligible(33, risk_levels, year_threshold)

    def get_all_eligible(self, risk_levels=None, year_threshold=None):
        """
        Get eligible taxpayers for all supported indicators.
        
        Returns:
            Dictionary with indicator numbers as keys and DataFrames as values
        """
        results = {}
        for ind in self.SUPPORTED_INDICATORS:
            try:
                results[f'ELIGIBLE_IND{ind}'] = self._get_eligible(ind, risk_levels, year_threshold)
            except ValueError:
                # Column doesn't exist, skip
                results[f'ELIGIBLE_IND{ind}'] = None
        return results