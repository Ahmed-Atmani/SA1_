class Match(val season: Int,
            val round: Int,
            val days_from_epoch: Int,
            val game_date: String,
            val day: String,
            val win_team: Team,
            val lose_team: Team,
            val win_pts: Int,
            val lose_pts: Int,
            val num_ot: Int,
            val academic_year: Int) extends Serializable:
  override def toString: String = s"Match(${win_team.name} [$win_pts-$lose_pts] ${lose_team.name})\n"

class Team(val seed: Int,
           val region: String ,
           val market: String ,
           val name: String ,
           val alias: String ,
           val team_id: String ,
           val school_ncaa: String ,
           val code_ncaa: Int,
           val kaggle_team_id: Int) extends Serializable:
  override def toString: String = s"Team($name)\n"
