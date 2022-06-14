from payload import Payload
import dshieldUtil

class GP:
    def __init__(self, gpId, lat, lon, gpType, biomeId):
        self.id = gpId
        self.lat = lat
        self.lon = lon
        self.type = int(gpType) # biome class
        self.biomeId = biomeId.strip() # biome id
        self.viewed = False
        self.measurementError = 0.04
        self.initialModelError = [] # from predictor
        self.finalModelError = []   # from plan execution
        self.accessTimes = []
        self.horizonAccessTimes = []
        self.filteredAccessTimes = []
        self.rainAccessTimes = {}
        self.rainHours = []  # 0-based index of hour when rain threshold is met
        self.isSaturated = False
        self.accessTimePairs = None
        self.pointingChoices = None # choices based on pointing options
        self.errorChoices   = None # flattened choices based on error table codes ("Aspect 1")
        self.errorTableChoices = None
        self.planChoice = None
        self.yVars =[]

    def isRainTime(self, tp):
        hour = tp//3600 # convert seconds to hours with floor division
        if hour in self.rainHours:
            return True
        else:
            return False


    def prettyPrint(self):
        msg = "[gp "+str(self.id)
        type = dshieldUtil.biomeLabel(self.type) if isinstance(self.type, int) else self.type
        msg += " type: "+type #self.typeLabel()
        msg += ", biomeId: "+self.biomeId #self.typeLabel()
        if self.rainHours:
            msg += ", rain: "+str(self.rainHours)
        if self.isSaturated:
            msg += ", saturated: "+str(self.viewed)
        if not self.measurementError == 0.04:
            msg += ", measurementErr: "+str(self.measurementError)
        if self.accessTimes:
            msg += ", accessTimes: "+str(sorted(self.accessTimes))
        if self.horizonAccessTimes:
            msg += ", horizonAccessTimes: "+str(self.horizonAccessTimes)
        if self.filteredAccessTimes:
            msg += ", filteredAccessTimes: "+str(self.filteredAccessTimes)
        if self.accessTimePairs:
            msg += ", accessTimePairs: "+str(self.accessTimePairs)
        if self.pointingChoices:
            msg += "\npointingChoices: "+str(self.pointingChoices)
        if self.errorChoices:
            msg += "\nerrorChoices: "+str(self.errorChoices)
        if self.errorTableChoices:
            msg += "\nerrorTableChoices ("+str(len(self.errorTableChoices))+"):\n"
            for choice in self.errorTableChoices:
                msg += str(choice)+"\n"
        if self.planChoice:
            msg += "planChoice: "+str(self.planChoice)
        else:
            msg += " * * UNPLANNED * *"
        msg += "\n\n"
        # if self.lat:
        #      msg += ", lat: "+str(self.lat)
        # if self.lon:
        #     msg += ", lon: " + str(self.lon)
        return msg

    def __str__(self):
        msg = "{'gp': "+str(self.id)

        msg += ", 'type': '"+str(self.type)+"'"
        msg += ", 'biomeId': '"+self.biomeId+"'"
        if self.lat:
             msg += ", 'lat': "+str(self.lat)
        if self.lon:
            msg += ", 'lon': " + str(self.lon)
        if self.rainHours:
            msg += ", 'rain': "+str(self.rainHours)
        if self.isSaturated:
            msg += ", 'saturated': "+str(self.viewed)
        if not self.measurementError == 0.04:
            msg += ", 'measurementErr': "+str(self.measurementError)
        if self.initialModelError:
            msg += ", 'initialModelError': "+str(self.initialModelError)
        if self.finalModelError:
            msg += ", 'finalModelError': "+str(self.finalModelError)
        if self.accessTimes:
            msg += ", 'accessTimes': "+str(sorted(self.accessTimes))
        if self.horizonAccessTimes:
            msg += ", 'horizonAccessTimes': "+str(self.horizonAccessTimes)
        if self.filteredAccessTimes:
            msg += ", 'filteredAccessTimes': "+str(self.filteredAccessTimes)
        if self.accessTimePairs:
            msg += ", 'accessTimePairs': "+str(self.accessTimePairs)
        if self.pointingChoices:
            msg += ", 'pointingChoices': "+str(self.pointingChoices)
        if self.errorChoices:
            msg += ", 'errorChoices': "+str(self.errorChoices)
        if self.errorTableChoices:
            msg += ", 'errorTableChoices': "+str(self.errorTableChoices)
        msg += "}"
        return msg